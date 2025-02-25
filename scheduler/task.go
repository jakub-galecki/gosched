package scheduler

import (
	"database/sql"
	"time"
)

const (
	insertTask = "INSERT INTO tasks(method, parameters, at) VALUES(?, ?, ?)"
	updAteTask = "UPDATE tasks SET completed=1 where id=?"
)

type Task struct {
	Id         int
	Method     string
	Parameters string

	At        time.Time
	Completed sql.NullBool
}

func (t *Task) isCompleted() bool {
	return t.Completed.Bool
}

func (t *Task) insert(tx *sql.Tx) (sql.Result, error) {
	return tx.Exec(insertTask, t.Method, t.Parameters, t.At)
}

func (t *Task) markAsDone(tx *sql.Tx) (sql.Result, error) {
	return tx.Exec(updAteTask, t.Id)
}
