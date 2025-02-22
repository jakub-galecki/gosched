package scheduler

import (
	"context"
	"database/sql"
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
	dbPath   string
	db       *sql.DB

	level     *slog.LevelVar
	logger    *slog.Logger
	exitChan  chan struct{}
	w         *worker // make it array, or create workers manager
	taskQueue chan *Task
    
    cache *fastcache.Cache // make iface 

    handler Handler

	opts struct {
		batch bool
	}
}

type Option func(*Scheduler)

type Options struct {
	endpoint string
}

func WithHandler(h Handler) Option {
	return func(s *Scheduler) {
		s.handler = h
	}
}

func WithDatabasePath(dbpath string) Option {
	return func(s *Scheduler) {
		s.dbPath = dbpath
	}
}

func WithDebugLevel(level slog.Level) Option {
	return func(s *Scheduler) {
		s.level.Set(level)
	}
}

// todo: self balancing workers reading database
// todo: cache completed tasks ??
// todo: register methods from file

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

	db, err := sql.Open("sqlite3", s.dbPath)
	if err != nil {
		s.logger.Error("error while opening database",
			slog.Any("err", err))
		return nil, err
	}
    
    s.cache = fastcache.New(4096) // 32MB by default
	s.db = db
	s.taskQueue = make(chan *Task)
	s.logger.Info("starting scheduler",
		slog.String("db_path", s.dbPath),
	)
	s.w = s.newWorker()
	return s, nil
}

func (s *Scheduler) Start() {
	go s.w.start()
	go startServer(s.taskQueue)
	s.logger.Debug("server started")
	for {
		select {
		case t := <-s.taskQueue:
			if s.opts.batch {
				// todo:
			}
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

	tx, err := s.db.BeginTx(ctx, nil)
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
