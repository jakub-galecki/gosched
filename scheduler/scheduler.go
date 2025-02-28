package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	_ "github.com/mattn/go-sqlite3"
)

const (
	dbName = "scheduler.db"
)

type Scheduler struct {
	db Database

	level     *slog.LevelVar
	logger    *slog.Logger
	exitChan  chan struct{}
	w         *worker // make it array, or create workers manager
	taskQueue chan *Task

	cache *fastcache.Cache // make iface

	handler Handler

	opts struct {
		batchSize int
		port      string
	}
}

type Option func(*Scheduler)

func WithHandler(h Handler) Option {
	return func(s *Scheduler) {
		s.handler = h
	}
}

func WithDatabase(db Database) Option {
	return func(s *Scheduler) {
		s.db = db
	}
}

func WithDebugLevel(level slog.Level) Option {
	return func(s *Scheduler) {
		s.level.Set(level)
	}
}

func WithBatchSize(sz int) Option {
	return func(s *Scheduler) {
		s.opts.batchSize = sz
	}
}

func WithPort(port string) Option {
	return func(s *Scheduler) {
		s.opts.port = port
	}
}

// todo: self balancing workers reading database
// todo: cache completed tasks ??

func NewScheduler(opts ...Option) (*Scheduler, error) {
	levelVar := new(slog.LevelVar)
	levelVar.Set(slog.LevelDebug)
	s := &Scheduler{
		level: new(slog.LevelVar),
		logger: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: levelVar,
		})),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.db == nil {
		return nil, errors.New("empty database")
	}
	s.cache = fastcache.New(4096) // 32MB by default
	s.taskQueue = make(chan *Task)
	s.logger.Info("starting scheduler")
	s.w = s.newWorker()
	return s, nil
}

func (s *Scheduler) Start() {
	go s.w.start()
	go startServer(s.taskQueue, s.opts.port)
	s.logger.Debug("server started")
	for {
		select {
		case t := <-s.taskQueue:
			err := s.register(t)
			if err != nil {
				s.logger.Error("error registering task",
					slog.Any("err", err),
					slog.String("method", t.Method),
					slog.String("parameters", t.Parameters),
					slog.Time("at", t.At))
			}
		case <-s.exitChan:
			s.w.exitChan <- struct{}{}
		}
	}
}

// todo: add batch register
func (s *Scheduler) register(t *Task) error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		s.logger.Error("couldn't begin transaction", slog.Any("err", err))
		return err
	}

	res, err := t.insert(tx)
	if err != nil {
		s.logger.Error("couldn't insert new task", slog.Any("err", err))
		tx.Rollback()
		return err
	}

	var (
		lastid, _ = res.LastInsertId()
		// rows, _   = res.RowsAffected()
	)

	s.logger.Debug("inserted new task",
		slog.Int64("insertedId", lastid))

	err = tx.Commit()
	if err != nil {
		s.logger.Error("error commiting transaction", slog.Any("err", err))
		return err
	}

	return nil
}
