package scheduler

import (
	"database/sql"
	"fmt"
	"os"
	"time"
)

const (
	N = 100

	_testDbFile = "./test_scheduler.db"
)

func setupDB() {
	os.Create(_testDbFile)
	db, err := sql.Open("sqlite3", _testDbFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	_, err = db.Exec(`CREATE TABLE "tasks" ("id" INTEGER,"method" TEXT NOT NULL,"parameters" TEXT NOT NULL,"at" datetime NOT NULL,"completed" INTEGER, PRIMARY KEY (id));`)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	db.Close()
}

func tearDown() {
	os.Remove(_testDbFile)
}

func makeDummyTasks() []*Task {
	method := "notify"
	at := time.Now()
	res := make([]*Task, N)
	for i := range N {
		res[i] = &Task{
			Id:         i,
			Method:     method,
			Parameters: "",
			At:         at.Add(5 * time.Second),
		}
	}
	return res
}

var _ Handler = (*TestHandler)(nil)

type TestHandler struct {
	count int
}

func (th *TestHandler) Handle(*Task) error {
	th.count += 1
	return nil
}

// func Test_Scheduler(t *testing.B) {
// 	setupDB()
// 	defer tearDown()

// 	s, err := NewScheduler(WithDatabasePath(_testDbFile))
// 	assert.NoError(t, err)

// 	for _, t := range makeDummyTasks() {
// 		s.taskQueue <- t
// 	}
// }
