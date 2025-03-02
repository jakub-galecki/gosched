package sqlitedb

import (
	"database/sql"
	"encoding/json"
	"github.com/gosched/scheduler"
	"time"
)

type Task struct {
	Id         int
	Method     string
	Parameters sql.RawBytes
	At         time.Time
	Completed  bool
}

func fromSchedulerTask(task *scheduler.Task) (*Task, error) {
	data, err := json.Marshal(task.Parameters)
	if err != nil {
		return nil, err
	}
	return &Task{
		Id:         task.Id,
		Method:     task.Method,
		Parameters: data,
		At:         task.At,
		Completed:  task.Completed,
	}, nil
}

func toSchedulerTask(task *Task, schedulerTask *scheduler.Task) error {
	var data map[string]string
	err := json.Unmarshal(task.Parameters, &data)
	if err != nil {
		return err
	}

	schedulerTask.Id = task.Id
	schedulerTask.Method = task.Method
	schedulerTask.Parameters = data
	schedulerTask.At = task.At
	schedulerTask.Completed = task.Completed
	return nil
}
