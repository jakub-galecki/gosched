package scheduler

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/fastcache"
)

const (
	ttime = 10 * time.Second
)

type worker struct {
	db       *sql.DB
	endpoint string

	ticker   *time.Ticker
	exitChan chan struct{}
	logger   *slog.Logger

	// shared among workers
	cache *fastcache.Cache
	h     Handler
}

func (s *Scheduler) newWorker() *worker {
	return &worker{
		db:       s.db,
		exitChan: make(chan struct{}),
		logger:   s.logger,
		ticker:   time.NewTicker(ttime),
		h:        s.handler,
	}
}

func (w *worker) findTasks() ([]*Task, error) {
	tt := time.Now()

	res, err := w.db.Query("SELECT * from tasks WHERE at < ? and (completed=0 or completed is null)", tt)
	if err != nil {
		return nil, err
	}
	var tasks []*Task

	for res.Next() {
		var task Task
		if err := res.Scan(&task.Id, &task.Method, &task.Parameters, &task.At, &task.Completed); err != nil {
			return nil, err
		}

		tasks = append(tasks, &task)
	}

	if err = res.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (w *worker) start() {
	for {
		select {
		case <-w.ticker.C:
			w.logger.Debug("im in ticker")
			tt, err := w.findTasks()
			if err != nil {
				w.logger.Error("error while finding tasks", slog.Any("err", err))
				w.ticker.Reset(ttime)
				continue
			}
			w.logger.Debug("found some tasks", slog.Int("number_of_tasks", len(tt)))
			for _, t := range tt {
                if err := w.h.Handle(t); err != nil {
                    w.logger.Error("error from handler", slog.Any("err", err))
                } else {
                    t.markAsDone(w.db)
                }
			}
		case <-w.exitChan:
			return
		}
	}
}
