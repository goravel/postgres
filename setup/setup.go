package main

import (
	"os"

	"github.com/goravel/framework/packages"
	"github.com/goravel/framework/packages/match"
	"github.com/goravel/framework/packages/modify"
	"github.com/goravel/framework/support/path"
)

var config = `map[string]any{
        "host":     config.Env("DB_HOST", "127.0.0.1"),
        "port":     config.Env("DB_PORT", 5432),
        "database": config.Env("DB_DATABASE", "forge"),
        "username": config.Env("DB_USERNAME", ""),
        "password": config.Env("DB_PASSWORD", ""),
        "sslmode":  "disable",
        "singular": false,
        "prefix":   "",
        "schema":   config.Env("DB_SCHEMA", "public"),
        "via": func() (driver.Driver, error) {
            return postgresfacades.Postgres("postgres")
        },

    }`

func main() {
	appConfigPath := path.Config("app.go")
	databaseConfigPath := path.Config("database.go")
	modulePath := packages.GetModulePath()
	postgresServiceProvider := "&postgres.ServiceProvider{}"
	driverContract := "github.com/goravel/framework/contracts/database/driver"
	postgresFacades := "github.com/goravel/postgres/facades"

	packages.Setup(os.Args).
		Install(
			// Add postgres service provider to app.go
			modify.GoFile(appConfigPath).
				Find(match.Imports()).Modify(modify.AddImport(modulePath)).
				Find(match.Providers()).Modify(modify.Register(postgresServiceProvider)),

			// Add postgres connection to database.go
			modify.GoFile(databaseConfigPath).Find(match.Imports()).Modify(
				modify.AddImport(driverContract),
				modify.AddImport(postgresFacades, "postgresfacades"),
			).
				Find(match.Config("database.connections")).Modify(modify.AddConfig("postgres", config)).
				Find(match.Config("http")).Modify(modify.AddConfig("default", `"gin"`)),
		).
		Uninstall(
			// Remove postgres connection from database.go
			modify.GoFile(databaseConfigPath).
				Find(match.Config("http")).Modify(modify.AddConfig("default", `""`)).
				Find(match.Config("database.connections")).Modify(modify.RemoveConfig("postgres")).
				Find(match.Imports()).Modify(
				modify.RemoveImport(driverContract),
				modify.RemoveImport(postgresFacades, "postgresfacades"),
			),

			// Remove postgres service provider from app.go
			modify.GoFile(appConfigPath).
				Find(match.Providers()).Modify(modify.Unregister(postgresServiceProvider)).
				Find(match.Imports()).Modify(modify.RemoveImport(modulePath)),
		).
		Execute()
}
