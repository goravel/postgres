package postgres

import (
	"fmt"

	"github.com/goravel/framework/contracts/config"
	"github.com/goravel/framework/contracts/database"
	"github.com/goravel/framework/contracts/database/driver"
	"github.com/goravel/framework/contracts/database/orm"
	contractsschema "github.com/goravel/framework/contracts/database/schema"
	"github.com/goravel/framework/contracts/log"
	"github.com/goravel/framework/contracts/testing"
	"github.com/goravel/framework/errors"
	"gorm.io/gorm"
)

var _ driver.Driver = &Postgres{}

type Postgres struct {
	config *ConfigBuilder
	log    log.Log
}

func NewPostgres(config config.Config, log log.Log, connection string) *Postgres {
	return &Postgres{
		config: NewConfigBuilder(config, connection),
		log:    log,
	}
}

func (r *Postgres) Config() database.Config {
	writers := r.config.Writes()
	if len(writers) == 0 {
		return database.Config{}
	}

	return database.Config{
		Connection: writers[0].Connection,
		Database:   writers[0].Database,
		Driver:     Name,
		Host:       writers[0].Host,
		Password:   writers[0].Password,
		Port:       writers[0].Port,
		Prefix:     writers[0].Prefix,
		Schema:     writers[0].Schema,
		Username:   writers[0].Username,
		Version:    r.version(),
	}
}

func (r *Postgres) Docker() (testing.DatabaseDriver, error) {
	writers := r.config.Writes()
	if len(writers) == 0 {
		return nil, errors.OrmDatabaseConfigNotFound
	}

	return NewDocker(writers[0].Database, writers[0].Username, writers[0].Password), nil
}

func (r *Postgres) Gorm() (*gorm.DB, driver.GormQuery, error) {
	db, err := NewGorm(r.config, r.log).Build()
	if err != nil {
		return nil, nil, err
	}

	return db, NewQuery(), nil
}

func (r *Postgres) Grammar() contractsschema.Grammar {
	return NewGrammar(r.config.Writes()[0].Prefix)
}

func (r *Postgres) Processor() contractsschema.Processor {
	return NewProcessor()
}

func (r *Postgres) Schema(orm orm.Orm) contractsschema.DriverSchema {
	return NewSchema(r.Grammar().(*Grammar), orm, r.config.Writes()[0].Schema, r.config.Writes()[0].Prefix)
}

func (r *Postgres) version() string {
	db, _, err := r.Gorm()
	if err != nil {
		return ""
	}

	var version struct {
		Value string
	}
	if err := db.Raw("SELECT current_setting('server_version') AS value;").Scan(&version).Error; err != nil {
		return fmt.Sprintf("UNKNOWN: %s", err)
	}

	return version.Value
}
