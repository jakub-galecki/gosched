package main

import (
	"errors"
	"flag"
	"os"

	"github.com/gosched/scheduler"
	sqlitedb "github.com/gosched/sqliteDb"
	"gopkg.in/yaml.v3"
)

type GroupingStrategy struct {
	Method            string   `yaml:"method"`
	TimeFormat        string   `yaml:"time_format"`
	GroupingParameter []string `yaml:"param"`
}

type Config struct {
	DatabaseType string `yaml:"database_type"`
	DatabasePath string `yaml:"database_path"`

	Port string `yaml:"port"`

	SinkType    string `yaml:"sink_type"`
	SinkAddress string `yaml:"sink_address"`

	SinkLog      string `yaml:"sink_log"`
	SchedulerLog string `yaml:"scheduler_log"`

	GroupingStrategy []GroupingStrategy `yaml:"grouping"`
}

func (c *Config) toOptions() ([]scheduler.Option, error) {
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

	if c.SinkLog == "" {
		return nil, errors.New("empty sink log")
	}

	if c.SchedulerLog == "" {
		return nil, errors.New("empty scheduler log")
	}

	m := make(map[string]scheduler.GroupingStrategy)
	for _, groupingStrategy := range c.GroupingStrategy {
		m[groupingStrategy.Method] = scheduler.GroupingStrategy{
			Method:     groupingStrategy.Method,
			TimeFormat: groupingStrategy.TimeFormat,
			Param:      groupingStrategy.GroupingParameter,
		}
	}

	db, err := sqlitedb.NewSqliteHandler("./scheduler/scheduler.db", true)
	if err != nil {
		panic(err)
	}

	return []scheduler.Option{
		scheduler.WithDatabase(db),
		scheduler.WithHandler(scheduler.NewHttpHandler(c.SinkAddress, c.SinkLog)),
		scheduler.WithPort(c.Port),
		scheduler.WithBatchSize(1000),
		scheduler.WithGroupingStrategy(m),
	}, nil
}

func main() {
	conff := flag.String("config_file", "", "path to a configuration file")

	flag.Parse()

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

	opts, err := conf.toOptions()
	if err != nil {
		panic(err)
	}

	s, err := scheduler.NewScheduler(conf.SchedulerLog, opts...)
	if err != nil {
		panic(err)
	}
	s.Start()
}
