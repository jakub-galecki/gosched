package scheduler

import (
	"context"
	"time"
)

type Iterator interface {
	Next() bool
	Scan(...any) error
	Into(*Task) error
	Err() error
	Close() error
}

type Result interface {
	LastInsertId() (int64, error)
}

type Transaction interface {
	Commit() error
	Rollback() error
	CompleteTask(id any) (Result, error)
	InsertTask(*Task) (Result, error)
	IncrementRetries(id any) (Result, error)
}

type Database interface {
	FindNotCompleted(time.Time) (Iterator, error)
	Begin(context.Context) (Transaction, error)
	InsertProcessed(any) (Result, error)
	GetProcessed() (Iterator, error)
}
