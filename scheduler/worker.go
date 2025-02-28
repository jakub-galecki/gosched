package scheduler

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"golang.org/x/sync/errgroup"
)

const (
	ttime                 = 10 * time.Second
	initialBatchSize      = 10
	btime                 = 5 * time.Second
	gorutinesHandlerLimit = 10
)

type worker struct {
	db Database

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
	res, err := w.db.FindNotCompleted(tt)
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
				w.finishTask(t)
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
	tx, err := w.db.Begin(context.Background())
	if err != nil {
		return err
	}

	var errg errgroup.Group
	errg.SetLimit(gorutinesHandlerLimit)
	stats := &stats{}
	for it.hasNext() {
		t := it.next()
		errg.Go(func() error {
			err = w.h.Handle(t)
			stats.add(err == nil)
			if err == nil {
				t.markAsDone(tx)
			}
			return err
		})
	}
	// todo: count number of retries
	// commit what can be commited
	err = errg.Wait()
	if err != nil {
		w.logger.Error("error handling task", slog.Any("err", err))
		return nil
	}
	err = tx.Commit()
	if err != nil {
		w.logger.Error("error commiting", slog.Any("err", err))
		return err
	}
	w.logger.Debug("commited batch",
		slog.Int64("time ms", time.Since(now).Milliseconds()),
		slog.Uint64("total", uint64(stats.total.Load())),
		slog.Uint64("succeed", uint64(stats.succeed.Load())),
		slog.Uint64("failed", uint64(stats.failed.Load())))
	return nil
}

type stats struct {
	total   atomic.Uint32
	succeed atomic.Uint32
	failed  atomic.Uint32
}

func (s *stats) add(ok bool) {
	s.total.Add(1)
	if ok {
		s.succeed.Add(1)
		return
	}
	s.failed.Add(1)
}
