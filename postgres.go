package postgres

import (
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/goravel/framework/contracts/config"
	"github.com/goravel/framework/contracts/database"
	"github.com/goravel/framework/contracts/database/driver"
	"github.com/goravel/framework/contracts/log"
	"github.com/goravel/framework/contracts/testing/docker"
	"github.com/goravel/framework/errors"
	"github.com/goravel/postgres/contracts"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var _ driver.Driver = &Postgres{}

type Postgres struct {
	config  contracts.ConfigBuilder
	db      *gorm.DB
	log     log.Log
	version string
}

func NewPostgres(config config.Config, log log.Log, connection string) *Postgres {
	return &Postgres{
		config: NewConfig(config, connection),
		log:    log,
	}
}

func (r *Postgres) Config() database.Config {
	writers := r.config.Writes()
	if len(writers) == 0 {
		return database.Config{}
	}

	return database.Config{
		Connection:        writers[0].Connection,
		Dsn:               writers[0].Dsn,
		Database:          writers[0].Database,
		Driver:            Name,
		Host:              writers[0].Host,
		Password:          writers[0].Password,
		Port:              writers[0].Port,
		Prefix:            writers[0].Prefix,
		Schema:            writers[0].Schema,
		Username:          writers[0].Username,
		Version:           r.getVersion(),
		PlaceholderFormat: sq.Dollar,
	}
}

func (r *Postgres) DB() (*sql.DB, error) {
	gormDB, err := r.Gorm()
	if err != nil {
		return nil, err
	}

	return gormDB.DB()
}

func (r *Postgres) Docker() (docker.DatabaseDriver, error) {
	writers := r.config.Writes()
	if len(writers) == 0 {
		return nil, errors.DatabaseConfigNotFound
	}

	return NewDocker(r.config, writers[0].Database, writers[0].Username, writers[0].Password), nil
}

func (r *Postgres) Explain(sql string, vars ...any) string {
	return postgres.New(postgres.Config{}).Explain(sql, vars...)
}

func (r *Postgres) Gorm() (*gorm.DB, error) {
	if r.db != nil {
		return r.db, nil
	}

	db, err := NewGorm(r.config, r.log).Build()
	if err != nil {
		return nil, err
	}

	r.db = db

	return db, nil
}

func (r *Postgres) Grammar() driver.Grammar {
	return NewGrammar(r.config.Writes()[0].Prefix)
}

func (r *Postgres) Processor() driver.Processor {
	return NewProcessor()
}

func (r *Postgres) getVersion() string {
	if r.version != "" {
		return r.version
	}

	instance, err := r.Gorm()
	if err != nil {
		return ""
	}

	var version struct {
		Value string
	}
	if err := instance.Raw("SELECT current_setting('server_version') AS value;").Scan(&version).Error; err != nil {
		r.version = fmt.Sprintf("UNKNOWN: %s", err)
	} else {
		r.version = version.Value
	}

	return r.version
}
