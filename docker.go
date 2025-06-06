package postgres

import (
	"fmt"
	"time"

	contractsdocker "github.com/goravel/framework/contracts/testing/docker"
	"github.com/goravel/framework/support/color"
	"github.com/goravel/framework/support/docker"
	"github.com/goravel/framework/support/process"
	"github.com/goravel/postgres/contracts"
	"gorm.io/driver/postgres"
	gormio "gorm.io/gorm"
)

type Docker struct {
	config      contracts.ConfigBuilder
	containerID string
	database    string
	host        string
	image       *contractsdocker.Image
	password    string
	username    string
	port        int
}

func NewDocker(config contracts.ConfigBuilder, database, username, password string) *Docker {
	return &Docker{
		config:   config,
		database: database,
		host:     "127.0.0.1",
		username: username,
		password: password,
		image: &contractsdocker.Image{
			Repository: "postgres",
			Tag:        "latest",
			Env: []string{
				"POSTGRES_USER=" + username,
				"POSTGRES_PASSWORD=" + password,
				"POSTGRES_DB=" + database,
			},
			ExposedPorts: []string{"5432"},
			Args:         []string{"-c max_connections=1000"},
		},
	}
}

func (r *Docker) Build() error {
	command, exposedPorts := docker.ImageToCommand(r.image)
	containerID, err := process.Run(command)
	if err != nil {
		return fmt.Errorf("init Postgres error: %v", err)
	}
	if containerID == "" {
		return fmt.Errorf("no container id return when creating Postgres docker")
	}

	r.containerID = containerID
	r.port = docker.ExposedPort(exposedPorts, 5432)

	return nil
}

func (r *Docker) Config() contractsdocker.DatabaseConfig {
	return contractsdocker.DatabaseConfig{
		ContainerID: r.containerID,
		Driver:      Name,
		Host:        r.host,
		Port:        r.port,
		Database:    r.database,
		Username:    r.username,
		Password:    r.password,
	}
}

func (r *Docker) Database(name string) (contractsdocker.DatabaseDriver, error) {
	go func() {
		gormDB, err := r.connect()
		if err != nil {
			color.Errorf("connect Postgres error: %v", err)
			return
		}

		res := gormDB.Exec(fmt.Sprintf(`CREATE DATABASE "%s";`, name))
		if res.Error != nil {
			color.Errorf("create Postgres database error: %v", res.Error)
		}

		if err := r.close(gormDB); err != nil {
			color.Errorf("close Postgres connection error: %v", err)
		}
	}()

	docker := NewDocker(r.config, name, r.username, r.password)
	docker.containerID = r.containerID
	docker.port = r.port

	return docker, nil
}

func (r *Docker) Driver() string {
	return Name
}

func (r *Docker) Fresh() error {
	gormDB, err := r.connect()
	if err != nil {
		return fmt.Errorf("connect Postgres error when clearing: %v", err)
	}

	if res := gormDB.Exec("DROP SCHEMA public CASCADE;"); res.Error != nil {
		return fmt.Errorf("drop schema of Postgres error: %v", res.Error)
	}

	if res := gormDB.Exec("CREATE SCHEMA public;"); res.Error != nil {
		return fmt.Errorf("create schema of Postgres error: %v", res.Error)
	}

	return r.close(gormDB)
}

func (r *Docker) Image(image contractsdocker.Image) {
	r.image = &image
}

func (r *Docker) Ready() error {
	gormDB, err := r.connect()
	if err != nil {
		return err
	}

	r.resetConfigPort()

	return r.close(gormDB)
}

func (r *Docker) Reuse(containerID string, port int) error {
	r.containerID = containerID
	r.port = port

	return nil
}

func (r *Docker) Shutdown() error {
	if _, err := process.Run(fmt.Sprintf("docker stop %s", r.containerID)); err != nil {
		return fmt.Errorf("stop Postgres error: %v", err)
	}

	return nil
}

func (r *Docker) connect() (*gormio.DB, error) {
	var (
		instance *gormio.DB
		err      error
	)

	// docker compose need time to start
	for i := 0; i < 60; i++ {
		instance, err = gormio.Open(postgres.New(postgres.Config{
			DSN: fmt.Sprintf("postgres://%s:%s@%s:%d/%s", r.username, r.password, r.host, r.port, r.database),
		}))

		if err == nil {
			break
		}

		time.Sleep(1 * time.Second)
	}

	return instance, err
}

func (r *Docker) close(gormDB *gormio.DB) error {
	db, err := gormDB.DB()
	if err != nil {
		return err
	}

	return db.Close()
}

func (r *Docker) resetConfigPort() {
	writers := r.config.Config().Get(fmt.Sprintf("database.connections.%s.write", r.config.Connection()))
	if writeConfigs, ok := writers.([]contracts.Config); ok {
		writeConfigs[0].Port = r.port
		r.config.Config().Add(fmt.Sprintf("database.connections.%s.write", r.config.Connection()), writeConfigs)

		return
	}

	r.config.Config().Add(fmt.Sprintf("database.connections.%s.port", r.config.Connection()), r.port)
}
