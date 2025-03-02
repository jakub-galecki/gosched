package scheduler

import (
	"bytes"
	"sync"
	"time"
)

var taskPool = &sync.Pool{
	New: func() any {
		return &Task{}
	},
}

type Task struct {
	Id         int
	Method     string
	Parameters map[string]string
	At         time.Time
	Completed  bool
}

func (t *Task) Dispose() {
	t.Id = -1
	t.Method = ""
	t.Parameters = make(map[string]string)
	t.At = time.Time{}
	t.Completed = false
	taskPool.Put(t)
}

func NewTask(id int, method string, params map[string]string, at time.Time, completed bool) *Task {
	t := taskPool.New().(*Task)
	t.Id = id
	t.Method = method
	t.Parameters = params
	t.At = at
	t.Completed = completed
	return t
}

func EmptyTask() *Task {
	return taskPool.New().(*Task)
}

func (t *Task) fromTask() *Task {
	tt := taskPool.New().(*Task)
	tt.Id = t.Id
	tt.Method = t.Method
	tt.Parameters = t.Parameters
	tt.At = t.At
	tt.Completed = t.Completed
	return tt
}

func (t *Task) insert(tx Transaction) (Result, error) {
	return tx.InsertTask(t)
}

func (t *Task) markAsDone(tx Transaction) (Result, error) {
	return tx.CompleteTask(t.Id)
}

func (t *Task) markAsFailed(tx Transaction) (Result, error) {
	return tx.IncrementRetries(t.Id)
}

func (t *Task) key(params []string, tformat string) []byte {
	var sb bytes.Buffer
	sb.WriteString(t.Method)
	sb.WriteString("_")
	for _, param := range params {
		sb.WriteString(t.Parameters[param])
		sb.WriteString("_")
	}
	sb.WriteString(t.At.Format(tformat))
	return sb.Bytes()
}
