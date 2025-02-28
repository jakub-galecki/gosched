package sqlitedb

import (
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/gosched/scheduler"
)

var (
	_ scheduler.Database    = (*sqliteHandler)(nil)
	_ scheduler.Transaction = (*transaction)(nil)
)

const (
	selectTask      = "SELECT * from tasks WHERE at < ? and (completed=0 or completed is null)"
	insertTask      = "INSERT INTO tasks(method, parameters, at) VALUES(?, ?, ?)"
	updateTask      = "UPDATE tasks SET completed=1 where id=?"
	createTaskTable = `CREATE TABLE "tasks" ("id" INTEGER,"method" TEXT NOT NULL,"parameters" TEXT NOT NULL,"at" datetime NOT NULL,"completed" INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (id));`
)

/*
	db, err := sql.Open("sqlite3", s.dbPath)
	if err != nil {
		s.logger.Error("error while opening database",
			slog.Any("err", err))
		return nil, err
	}


*/

type sqliteHandler struct {
	db *sql.DB
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

func NewSqliteHandler(path string) (*sqliteHandler, error) {
	err := ensureFile(path)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	return &sqliteHandler{
		db: db,
	}, nil
}

type it struct {
	*sql.Rows
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

type transaction struct {
	*sql.Tx
}

func (t *transaction) CompleteTask(id int) (scheduler.Result, error) {
	return t.Exec(updateTask, id)
}

func (t *transaction) InsertTask(task *scheduler.Task) (scheduler.Result, error) {
	return t.Exec(insertTask, task.Method, task.Parameters, task.At)
}

func (h *sqliteHandler) Begin(ctx context.Context) (scheduler.Transaction, error) {
	tx, err := h.db.Begin()
	if err != nil {
		return nil, err
	}
	return &transaction{
		Tx: tx,
	}, nil
}
