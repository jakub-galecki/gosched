# Scheduler written in GoLang

Program for scheduling tasks that should be executed at given time.
Scheduler stores tasks in the underlying database that can be defined by the user. 
There is a sqlite3 wrapper, that exposes api for scheduler to operate on actions,
but any database can be used as long as the implementation satisfies interfaces from `scheduler/database.go`.

Currently, scheduler exposes http and grpc api that can be used to register tasks. For example with http one can 
register new task in the following way:

```bash
curl --request POST \
  --url http://localhost:8080/scheduler.SchedulerServer/Register \
  --header 'content-type: application/json' \
  --data '{
  "method": "notify",
  "params": {
    "name": "zuzia"
  },
  "at": "2025-02-26T19:10:00+01:00"
}'
```

Scheduler runs background process that scans the database in the time intervals configured by the method `WithTicker`.
When any tasks are found whose `at` has passed they are send to their destination by the configured handler. Currently,
there is a http handler but implementation can be provided by the user by the `WithHandler` method. 

Tasks can be configured to be grouped by theirs method and parameters. Different strategies for tasks grouping are configured
per method and execution time. For example one can configure scheduler to send only one task per user with `id` at given day.


#### Example configuration:

```yaml
database_type: sqlite
database_path: ./scheduler/scheduler.db
port: ":8080"
sink_type: http
sink_address: "http://localhost:9000"
sink_log: "./sink.log"
scheduler_log: "./scheduler.log"
grouping:
  - name: notify
    method: notify
    time_format: 20060102
    param:
      - name
```

This configuration sets the database type to sqlite and specifies http handler to execute tasks. Tasks with method notfiy
are being grouping by their `name` parameter and by the `year-month-day` of execution time. 
So if there are multiple tasks with the same name scheduled for the same day only one would be executed.
