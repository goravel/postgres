package postgres

import (
	"gorm.io/gorm"

	"github.com/goravel/framework/contracts/config"
	"github.com/goravel/framework/contracts/database"
	"github.com/goravel/framework/contracts/database/driver"
	"github.com/goravel/framework/contracts/database/orm"
	contractsschema "github.com/goravel/framework/contracts/database/schema"
	"github.com/goravel/framework/contracts/log"
	"github.com/goravel/framework/contracts/testing"
	"github.com/goravel/framework/errors"
)

var _ driver.Driver = &Postgres{}

type Postgres struct {
	config *ConfigBuilder
	log    log.Log
	orm    orm.Orm
}

func NewPostgres(config config.Config, log log.Log, orm orm.Orm, connection string) *Postgres {
	return &Postgres{
		config: NewConfigBuilder(config, connection),
		log:    log,
		orm:    orm,
	}
}

func (r *Postgres) Config() database.Config {
	writers := r.config.Writes()
	if len(writers) == 0 {
		return database.Config{}
	}

	return database.Config{
		Connection: r.config.Connection(),
		Driver:     DriverName,
		Prefix:     writers[0].Prefix,
		Schema:     writers[0].Schema,
	}
}

func (r *Postgres) Docker() (testing.DatabaseDriver, error) {
	writers := r.config.Writes()
	if len(writers) == 0 {
		return nil, errors.OrmDatabaseConfigNotFound
	}

	return NewDocker(writers[0].Database, writers[0].Username, writers[0].Password), nil
}

func (r *Postgres) Gorm() (*gorm.DB, error) {
	return NewGorm(r.config, r.log).Build()
}

func (r *Postgres) Grammar() contractsschema.Grammar {
	return NewGrammar(r.config.Writes()[0].Prefix)
}

func (r *Postgres) Processor() contractsschema.Processor {
	return NewProcessor()
}

func (r *Postgres) Schema() contractsschema.DriverSchema {
	return NewSchema(r.Grammar().(*Grammar), r.orm, r.config.Writes()[0].Schema, r.config.Writes()[0].Prefix)
}
