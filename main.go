package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/gosched/scheduler"
)

var tasks = []*scheduler.Task{
	{
		Method:     "notify",
		Parameters: "{person: zuzia}",
		At:         time.Now().Add(time.Hour),
	},
	{
		Method:     "notify",
		Parameters: "{person: zuzia}",
		At:         time.Now().Add(time.Hour),
	},
}

func main() {
	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
    s, err := scheduler.NewScheduler(scheduler.WithHandler(scheduler.NewHttpHandler("http://localhost:9000", l)),
		scheduler.WithDatabasePath("./scheduler/scheduler.db"))
	if err != nil {
		panic(err)
	}
	s.Start()
}
