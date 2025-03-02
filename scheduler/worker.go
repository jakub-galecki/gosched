package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"golang.org/x/sync/errgroup"
)

const (
	ttime                 = 10 * time.Second
	initialBatchSize      = 10
	btime                 = 5 * time.Second
	gorutinesHandlerLimit = 20
)

type worker struct {
	db Database

	ticker   *time.Ticker
	exitChan chan struct{}
	logger   *slog.Logger
	ttime    time.Duration
	// shared among workers
	cache         *fastcache.Cache
	h             Handler
	batchLiveTime *time.Ticker // time after which batch will by commited manually
	batch         *batch
	batchSize     int
	strategy      map[string]GroupingStrategy
	bmu           sync.Mutex
	grouped       chan []byte
}

func (s *Scheduler) newWorker() (*worker, error) {
	w := &worker{
		db:            s.db,
		exitChan:      make(chan struct{}),
		logger:        s.logger,
		h:             s.handler,
		batchLiveTime: time.NewTicker(btime),
		batchSize:     s.opts.batchSize,
		strategy:      s.opts.groupingStrategy,
		grouped:       make(chan []byte),
		cache:         s.cache,
	}
	if s.opts.ticker != nil {
		w.ttime = *s.opts.ticker
	} else {
		w.ttime = ttime
	}
	w.ticker = time.NewTicker(w.ttime)
	err := w.initCache()
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (w *worker) initCache() error {
	rows, err := w.db.GetProcessed()
	if err != nil {
		return err
	}
	var (
		key []byte
	)

	var (
		keys []string
	)

	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&key); err != nil {
			return err
		}

		w.cache.Set(key, nil)
		keys = append(keys, string(key))
	}

	w.logger.Debug("initialized cache",
		slog.Any("keys", keys))

	return nil
}

func (w *worker) findTasks() ([]*Task, error) {
	tt := time.Now()

	res, err := w.db.FindNotCompleted(tt)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = res.Close()
		if err != nil {
			w.logger.Error("error while closing iterator", slog.Any("error", err))
		}
	}()

	var (
		tasks []*Task
	)

	for res.Next() {
		t := EmptyTask()
		err := res.Into(t)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, t)
	}

	if err = res.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (w *worker) start() {
	go w.groupedWorker()
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

func (w *worker) groupedWorker() {
	for key := range w.grouped {
		res, err := w.db.InsertProcessed(key)
		if err != nil {
			w.logger.Error("error while inserting processed", slog.Any("error", err))
		} else {
			id, _ := res.LastInsertId()
			w.logger.Debug("inserted processed key",
				slog.String("key", string(key)),
				slog.Int64("id", id))
		}
	}
}

func (w *worker) finishTask(t *Task) {
	if w.batch == nil {
		w.batch = newBatch(w.batchSize, w.strategy, w.cache, w.grouped)
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

	w.bmu.Lock()
	defer w.bmu.Unlock()

	defer func() {
		b.reset()
	}()

	if len(b.tasks) == 0 && len(b.excluded) == 0 {
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
				_, err = t.markAsDone(tx)
				if err != nil {
					w.logger.Error("error while marking task as done", slog.Any("error", err))
				}
			} else {
				_, err = t.markAsFailed(tx)
				if err != nil {
					w.logger.Error("error while marking task as failed", slog.Any("error", err))
				}
			}
			t.Dispose()
			return err
		})
	}

	if len(b.excluded) > 0 {
		errg.Go(func() error {
			for _, t := range b.excluded {
				_, err = t.markAsDone(tx)
				if err != nil {
					w.logger.Error("error while marking excluded",
						slog.Any("error", err))
				}
			}
			return nil
		})

	}

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
		slog.Float64("time s", time.Since(now).Seconds()),
		slog.Uint64("total", uint64(stats.total.Load())),
		slog.Uint64("succeed", uint64(stats.succeed.Load())),
		slog.Uint64("failed", uint64(stats.failed.Load())),
		slog.Int("grouped", len(b.excluded)))
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
