package scheduler

import (
	"time"
)

type Task struct {
	Id         int
	Method     string
	Parameters string

	At        time.Time
	Completed bool
}

func (t *Task) insert(tx Transaction) (Result, error) {
	return tx.InsertTask(t)
}

func (t *Task) markAsDone(tx Transaction) (Result, error) {
	return tx.CompleteTask(t.Id)
}
