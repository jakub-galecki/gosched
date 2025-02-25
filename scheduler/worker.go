package scheduler

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/hashicorp/go-multierror"
)

const (
	ttime            = 10 * time.Second
	initialBatchSize = 10
	btime            = 5 * time.Second
)

type worker struct {
	db *sql.DB

	ticker   *time.Ticker
	exitChan chan struct{}
	logger   *slog.Logger

	// shared among workers
	cache         *fastcache.Cache
	h             Handler
	batchLiveTime *time.Ticker // time after which batch will by commited manually
	batch         *batch
	batchSize     int
}

func (s *Scheduler) newWorker() *worker {
	return &worker{
		db:            s.db,
		exitChan:      make(chan struct{}),
		logger:        s.logger,
		ticker:        time.NewTicker(ttime),
		h:             s.handler,
		batchLiveTime: time.NewTicker(btime),
		batchSize:     s.opts.batchSize,
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
					w.finishTask(t)
				}
			}
		case <-w.batchLiveTime.C:
			w.logger.Debug("im in batch ticker to commit")
			w.commitBatch(w.batch)
		case <-w.exitChan:
			return
		}
	}
}

func (w *worker) finishTask(t *Task) {
	if w.batch == nil {
		w.batch = newBatch(w.batchSize)
	}

	if w.batch.ready() {
		w.commitBatch(w.batch)
	}

	w.batch.add(t)
}

func (w *worker) commitBatch(b *batch) error {
	if b == nil {
		return nil
	}

	defer func() {
		b.reset()
	}()

	if len(b.tasks) == 0 {
		return nil
	}

	now := time.Now()

	it := b.iter()
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	var errg *multierror.Error
	for it.hasNext() {
		t := it.next()
		err = w.h.Handle(t)
		if err == nil {
			t.markAsDone(tx)
		}
		errg = multierror.Append(errg, err)
	}
	// commit what can be commited
	err = tx.Commit()
	w.logger.Debug("[commitBatch] took", slog.Int64("time ms", time.Since(now).Milliseconds()))
	return multierror.Append(errg, err).ErrorOrNil()
}
