package scheduler

type batch struct {
	tasks   []*Task
	maxSize int
}

func newBatch(maxSize int) *batch {
	return &batch{
		tasks:   make([]*Task, 0, maxSize),
		maxSize: maxSize,
	}
}

func (b *batch) add(t *Task) {
	b.tasks = append(b.tasks, t)
}

func (b *batch) reset() {
	b.tasks = b.tasks[:0]
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
