package postgres

import (
	"fmt"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"gorm.io/plugin/dbresolver"

	"github.com/goravel/framework/contracts/database"
	"github.com/goravel/framework/contracts/database/orm"
	"github.com/goravel/framework/contracts/log"
	"github.com/goravel/framework/contracts/testing"
	databasegorm "github.com/goravel/framework/database/gorm"
	"github.com/goravel/framework/errors"
	"github.com/goravel/framework/support/carbon"
	"github.com/goravel/postgres/contracts"
)

var _ orm.Driver = &Postgres{}

type Postgres struct {
	configBuilder *ConfigBuilder
	log           log.Log
}

func NewPostgres(configBuilder *ConfigBuilder, log log.Log) *Postgres {
	return &Postgres{
		configBuilder: configBuilder,
		log:           log,
	}
}

func (r *Postgres) Config() database.Config1 {
	writers := r.configBuilder.Writes()
	if len(writers) == 0 {
		return database.Config1{}
	}

	return database.Config1{
		Connection: r.configBuilder.Connection(),
		Driver:     DriverName,
		Prefix:     writers[0].Prefix,
	}
}

func (r *Postgres) Docker() (testing.DatabaseDriver, error) {
	writers := r.configBuilder.Writes()
	if len(writers) == 0 {
		return nil, errors.OrmDatabaseConfigNotFound
	}

	return NewDocker(writers[0].Database, writers[0].Username, writers[0].Password), nil
}

func (r *Postgres) Gorm() (*gorm.DB, error) {
	instance, err := r.instance()
	if err != nil {
		return nil, err
	}
	if err := r.configurePool(instance); err != nil {
		return nil, err
	}
	if err := r.configureReadWriteSeparate(instance); err != nil {
		return nil, err
	}

	return instance, nil
}

func (r *Postgres) configsToDialectors(configs []contracts.FullConfig) ([]gorm.Dialector, error) {
	var dialectors []gorm.Dialector

	for _, config := range configs {
		dsn := r.dns(config)
		if dsn == "" {
			return nil, errors.New("failed to generate DSN, please check the database configuration")
		}

		dialector := postgres.New(postgres.Config{
			DSN: dsn,
		})

		dialectors = append(dialectors, dialector)
	}

	return dialectors, nil
}

func (r *Postgres) configurePool(instance *gorm.DB) error {
	db, err := instance.DB()
	if err != nil {
		return err
	}

	db.SetMaxIdleConns(r.configBuilder.Config().GetInt("database.pool.max_idle_conns", 10))
	db.SetMaxOpenConns(r.configBuilder.Config().GetInt("database.pool.max_open_conns", 100))
	db.SetConnMaxIdleTime(time.Duration(r.configBuilder.Config().GetInt("database.pool.conn_max_idletime", 3600)) * time.Second)
	db.SetConnMaxLifetime(time.Duration(r.configBuilder.Config().GetInt("database.pool.conn_max_lifetime", 3600)) * time.Second)

	return nil
}

func (r *Postgres) configureReadWriteSeparate(instance *gorm.DB) error {
	writers, readers, err := r.writerAndReaderDialectors()
	if err != nil {
		return err
	}

	return instance.Use(dbresolver.Register(dbresolver.Config{
		Sources:           writers,
		Replicas:          readers,
		Policy:            dbresolver.RandomPolicy{},
		TraceResolverMode: true,
	}))
}

func (r *Postgres) dns(config contracts.FullConfig) string {
	if config.Dsn != "" {
		return config.Dsn
	}
	if config.Host == "" {
		return ""
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&timezone=%s&search_path=%s",
		config.Username, config.Password, config.Host, config.Port, config.Database, config.Sslmode, config.Timezone, config.Schema)
}

func (r *Postgres) gormConfig() *gorm.Config {
	logger := databasegorm.NewLogger(r.configBuilder.Config(), r.log)
	writeConfigs := r.configBuilder.Writes()
	if len(writeConfigs) == 0 {
		return nil
	}

	return &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		SkipDefaultTransaction:                   true,
		Logger:                                   logger,
		NowFunc: func() time.Time {
			return carbon.Now().StdTime()
		},
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   writeConfigs[0].Prefix,
			SingularTable: writeConfigs[0].Singular,
			NoLowerCase:   writeConfigs[0].NoLowerCase,
			NameReplacer:  writeConfigs[0].NameReplacer,
		},
	}
}

func (r *Postgres) instance() (*gorm.DB, error) {
	writers, _, err := r.writerAndReaderDialectors()
	if err != nil {
		return nil, err
	}
	if len(writers) == 0 {
		return nil, errors.OrmDatabaseConfigNotFound
	}

	instance, err := gorm.Open(writers[0], r.gormConfig())
	if err != nil {
		return nil, err
	}

	return instance, nil
}

func (r *Postgres) writerAndReaderDialectors() (writers []gorm.Dialector, readers []gorm.Dialector, err error) {
	writeConfigs := r.configBuilder.Writes()
	readConfigs := r.configBuilder.Reads()

	writers, err = r.configsToDialectors(writeConfigs)
	if err != nil {
		return nil, nil, err
	}
	readers, err = r.configsToDialectors(readConfigs)
	if err != nil {
		return nil, nil, err
	}

	return writers, readers, nil
}
