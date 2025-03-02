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

type GroupingStrategy struct {
	Method     string
	TimeFormat string
	Param      []string
}

type Scheduler struct {
	db Database

	level     *slog.LevelVar
	logger    *slog.Logger
	exitChan  chan struct{}
	w         *worker // make it array, or create workers manager
	taskQueue chan *Task

	cache *fastcache.Cache // make iface

	handler Handler
	logfile *os.File

	opts struct {
		batchSize        int
		port             string
		groupingStrategy map[string]GroupingStrategy
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

func WithGroupingStrategy(m map[string]GroupingStrategy) Option {
	return func(s *Scheduler) {
		s.opts.groupingStrategy = m
	}
}

func NewScheduler(logPath string, opts ...Option) (*Scheduler, error) {
	levelVar := new(slog.LevelVar)
	levelVar.Set(slog.LevelDebug)
	var out *os.File
	var err error
	if logPath != "" {
		out, err = os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
	} else {
		out = os.Stdout
	}
	s := &Scheduler{
		level: new(slog.LevelVar),
		logger: slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{
			Level: levelVar,
		})),
		logfile: out,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.db == nil {
		return nil, errors.New("empty database")
	}
	s.cache = fastcache.New(4096) // 32MB by default
	s.taskQueue = make(chan *Task)
	s.logger.Info("starting scheduler",
		slog.Any("strategy", s.opts.groupingStrategy))
	s.w, err = s.newWorker()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Scheduler) Start() {
	go s.w.start()
	go func() {
		err := startServer(s.taskQueue, s.opts.port)
		if err != nil {
			s.logger.Error("error initializing server", slog.Any("error", err))
			return
		}
	}()
	//sigs := make(chan os.Signal, 1)
	//signal.Notify(sigs, syscall.SIGINT)
	s.logger.Debug("server started")
	for {
		select {
		case t := <-s.taskQueue:
			err := s.register(t)
			if err != nil {
				s.logger.Error("error registering task",
					slog.Any("err", err),
					slog.String("method", t.Method),
					slog.Any("parameters", t.Parameters),
					slog.Time("at", t.At))
			}
		//case <-sigs:
		//	s.logger.Info("received shutdown signal")
		//	s.exitChan <- struct{}{}
		case <-s.exitChan:
			s.w.exitChan <- struct{}{}
			return
		}
	}
}

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
