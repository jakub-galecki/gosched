package scheduler

import "github.com/VictoriaMetrics/fastcache"

type batch struct {
	tasks    []*Task
	maxSize  int
	strategy map[string]GroupingStrategy
	cache    *fastcache.Cache
	grouped  chan []byte
	excluded []*Task
}

func newBatch(maxSize int, st map[string]GroupingStrategy, c *fastcache.Cache, grouped chan []byte) *batch {
	return &batch{
		tasks:    make([]*Task, 0, maxSize),
		maxSize:  maxSize,
		strategy: st,
		cache:    c,
		grouped:  grouped,
	}
}

func (b *batch) add(t *Task) {
	if g, ok := b.strategy[t.Method]; ok {
		key := t.key(g.Param, g.TimeFormat)
		if b.cache.Has(key) {
			b.excluded = append(b.excluded, t)
			return
		} else {
			b.cache.Set(key, nil)
			b.grouped <- key
		}
	}
	b.tasks = append(b.tasks, t)
}

func (b *batch) reset() {
	b.tasks = b.tasks[:0]
	b.excluded = b.excluded[:0]
}

func (b *batch) size() int {
	return len(b.tasks)
}

func (b *batch) ready() bool {
	return len(b.tasks) == b.maxSize
}

type batchIterator struct {
	*batch
	cursor int
}

func (b *batch) iter() *batchIterator {
	return &batchIterator{
		cursor: -1,
		batch:  b,
	}
}

func (it *batchIterator) hasNext() bool {
	return it.cursor+1 < len(it.tasks)
}

func (it *batchIterator) next() *Task {
	it.cursor++
	return it.tasks[it.cursor]
}
