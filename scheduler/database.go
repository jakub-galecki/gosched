package scheduler

import (
	"context"
	"time"
)

type Iterator interface {
	Next() bool
	Scan(...any) error
	Err() error
}

type Result interface {
	LastInsertId() (int64, error)
}

type Transaction interface {
	Commit() error
	Rollback() error
	CompleteTask(id int) (Result, error)
	InsertTask(*Task) (Result, error)
}

type Database interface {
	FindNotCompleted(time.Time) (Iterator, error)
	Begin(context.Context) (Transaction, error)
}
