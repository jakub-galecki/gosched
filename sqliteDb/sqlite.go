package sqlitedb

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"

	"github.com/gosched/scheduler"
)

var (
	_ scheduler.Database    = (*sqliteHandler)(nil)
	_ scheduler.Transaction = (*transaction)(nil)
	_ scheduler.Transaction = (*singleTransaction)(nil)
)

const (
	selectTask      = "SELECT * from tasks WHERE at < ? and (completed=0 or completed is null)"
	insertTask      = "INSERT INTO tasks(method, parameters, at) VALUES(?, ?, ?)"
	updateTask      = "UPDATE tasks SET completed=1 where id=?"
	createTaskTable = `CREATE TABLE "tasks" ("id" integer,"method" TEXT NOT NULL,"parameters" TEXT NOT NULL,"at" datetime NOT NULL, "completed" INTEGER NOT NULL DEFAULT 0, "retries" INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (id));`
	incrRetries     = "UPDATE tasks SET retires = retries+1 WHERE id=?"
	createProcessed = `CREATE TABLE processed("id" integer , "key" TEXT not null, "at" datetime not null default CURRENT_TIMESTAMP, PRIMARY KEY (id));`
	insertProcessed = "INSERT INTO processed(key) VALUES(?)"
	getProcessed    = "SELECT key FROM processed"
)

type sqliteHandler struct {
	db                *sql.DB
	singleTransaction bool
}

func ensureFile(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Create an empty file
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		defer file.Close()
	}
	return nil
}

func NewSqliteHandler(path string, singleTransactions bool) (*sqliteHandler, error) {
	err := ensureFile(path)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	// todo create tables if not exist

	return &sqliteHandler{
		db:                db,
		singleTransaction: singleTransactions,
	}, nil
}

type it struct {
	*sql.Rows
}

func (i *it) Into(task *scheduler.Task) error {
	tmpTask := &Task{}
	retries := 0

	if err := i.Rows.Scan(&tmpTask.Id, &tmpTask.Method, &tmpTask.Parameters, &tmpTask.At, &tmpTask.Completed, &retries); err != nil {
		return err
	}

	if err := toSchedulerTask(tmpTask, task); err != nil {
		return err
	}

	tmpTask = nil
	return nil

}

func (h *sqliteHandler) FindNotCompleted(at time.Time) (scheduler.Iterator, error) {
	rows, err := h.db.Query(selectTask, at)
	if err != nil {
		return nil, err
	}
	return &it{
		Rows: rows,
	}, nil
}

func (h *sqliteHandler) Begin(ctx context.Context) (scheduler.Transaction, error) {
	if h.singleTransaction {
		return &singleTransaction{
			DB: h.db,
		}, nil
	}
	tx, err := h.db.Begin()
	if err != nil {
		return nil, err
	}
	return &transaction{
		Tx: tx,
	}, nil
}

func (h *sqliteHandler) InsertProcessed(key any) (scheduler.Result, error) {
	return h.db.Exec(insertProcessed, key)
}

func (h *sqliteHandler) GetProcessed() (scheduler.Iterator, error) {
	rows, err := h.db.Query(getProcessed)
	if err != nil {
		return nil, err
	}
	return &it{
		Rows: rows,
	}, nil
}

type transaction struct {
	*sql.Tx
}

func (t *transaction) CompleteTask(id any) (scheduler.Result, error) {
	iid, ok := id.(int)
	if !ok {
		return nil, errors.New("invalid id type")
	}
	return t.Exec(updateTask, iid)
}

func (t *transaction) InsertTask(task *scheduler.Task) (scheduler.Result, error) {
	ttask, err := fromSchedulerTask(task)
	if err != nil {
		return nil, err
	}
	return t.Exec(insertTask, ttask.Method, ttask.Parameters, ttask.At)
}

func (t *transaction) IncrementRetries(id any) (scheduler.Result, error) {
	iid, ok := id.(int)
	if !ok {
		return nil, errors.New("invalid id type")
	}
	return t.Exec(incrRetries, iid)
}

type singleTransaction struct {
	*sql.DB
}

func (t *singleTransaction) CompleteTask(id any) (scheduler.Result, error) {
	iid, ok := id.(int)
	if !ok {
		return nil, errors.New("invalid id type")
	}
	return t.Exec(updateTask, iid)
}

func (t *singleTransaction) InsertTask(task *scheduler.Task) (scheduler.Result, error) {
	ttask, err := fromSchedulerTask(task)
	if err != nil {
		return nil, err
	}
	return t.Exec(insertTask, ttask.Method, ttask.Parameters, ttask.At)
}

func (t *singleTransaction) IncrementRetries(id any) (scheduler.Result, error) {
	iid, ok := id.(int)
	if !ok {
		return nil, errors.New("invalid id type")
	}
	return t.Exec(incrRetries, iid)
}

func (t *singleTransaction) Commit() error {
	return nil
}

func (t *singleTransaction) Rollback() error {
	return nil
}
