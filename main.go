package main

import (
	"errors"
	"flag"
	"log/slog"
	"os"

	"github.com/gosched/scheduler"
	sqlitedb "github.com/gosched/sqliteDb"
	"gopkg.in/yaml.v3"
)

type Config struct {
	DatabaseType string
	DatabasePath string

	Port string

	SinkType    string
	SinkAddress string
}

// todo reformat
func (c *Config) toOptions(l *slog.Logger) ([]scheduler.Option, error) {
	if c.DatabaseType != "sqlite" {
		return nil, errors.New("unsuported database type")
	}

	if c.DatabasePath == "" {
		return nil, errors.New("empty database path")
	}

	if c.Port == "" {
		return nil, errors.New("empty port")
	}

	if c.SinkType != "http" {
		return nil, errors.New("unsuported sink type")
	}

	if c.SinkAddress == "" {
		return nil, errors.New("empty sink address")
	}

	db, err := sqlitedb.NewSqliteHandler("./scheduler/scheduler.db")
	if err != nil {
		panic(err)
	}

	return []scheduler.Option{
		scheduler.WithDatabase(db),
		scheduler.WithHandler(scheduler.NewHttpHandler(c.SinkAddress, l)),
		scheduler.WithPort(c.Port),
	}, nil
}

func main() {

	conff := flag.String("config_file", "", "path to a configuration file")
	if conff == nil || *conff == "" {
		panic(errors.New("empty config file"))
	}

	rawConf, err := os.ReadFile(*conff)
	if err != nil {
		panic(err)
	}

	conf := Config{}
	err = yaml.Unmarshal(rawConf, &conf)
	if err != nil {
		panic(err)
	}

	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	opts, err := conf.toOptions(l)
	if err != nil {
		panic(err)
	}

	s, err := scheduler.NewScheduler(opts...)
	if err != nil {
		panic(err)
	}
	s.Start()
}
