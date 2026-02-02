package main

import (
	"os"

	"github.com/goravel/framework/packages"
	"github.com/goravel/framework/packages/match"
	"github.com/goravel/framework/packages/modify"
	"github.com/goravel/framework/support/env"
	"github.com/goravel/framework/support/path"
)

func main() {
	setup := packages.Setup(os.Args)
	config := `map[string]any{
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

	appConfigPath := path.Config("app.go")
	databaseConfigPath := path.Config("database.go")
	moduleImport := setup.Paths().Module().Import()
	postgresServiceProvider := "&postgres.ServiceProvider{}"
	driverContract := "github.com/goravel/framework/contracts/database/driver"
	postgresFacades := "github.com/goravel/postgres/facades"
	databaseConnectionsConfig := match.Config("database.connections")
	databaseConfig := match.Config("database")

	setup.Install(
		// Add postgres service provider to app.go if not using bootstrap setup
		modify.When(func(_ map[string]any) bool {
			return !env.IsBootstrapSetup()
		}, modify.GoFile(appConfigPath).
			Find(match.Imports()).Modify(modify.AddImport(moduleImport)).
			Find(match.Providers()).Modify(modify.Register(postgresServiceProvider))),

		// Add postgres service provider to providers.go if using bootstrap setup
		modify.When(func(_ map[string]any) bool {
			return env.IsBootstrapSetup()
		}, modify.RegisterProvider(moduleImport, postgresServiceProvider)),

		// Add postgres connection to database.go
		modify.GoFile(databaseConfigPath).Find(match.Imports()).Modify(
			modify.AddImport(driverContract),
			modify.AddImport(postgresFacades, "postgresfacades"),
		).
			Find(databaseConnectionsConfig).Modify(modify.AddConfig("postgres", config)).
			Find(databaseConfig).Modify(modify.AddConfig("default", `"postgres"`)),
	).Uninstall(
		// Remove postgres connection from database.go
		modify.WhenFileExists(databaseConfigPath, modify.GoFile(databaseConfigPath).
			Find(databaseConfig).Modify(modify.AddConfig("default", `""`)).
			Find(databaseConnectionsConfig).Modify(modify.RemoveConfig("postgres")).
			Find(match.Imports()).Modify(
			modify.RemoveImport(driverContract),
			modify.RemoveImport(postgresFacades, "postgresfacades"),
		)),

		// Remove postgres service provider from app.go if not using bootstrap setup
		modify.When(func(_ map[string]any) bool {
			return !env.IsBootstrapSetup()
		}, modify.GoFile(appConfigPath).
			Find(match.Providers()).Modify(modify.Unregister(postgresServiceProvider)).
			Find(match.Imports()).Modify(modify.RemoveImport(moduleImport))),

		// Remove postgres service provider from providers.go if using bootstrap setup
		modify.When(func(_ map[string]any) bool {
			return env.IsBootstrapSetup()
		}, modify.UnregisterProvider(moduleImport, postgresServiceProvider)),
	).Execute()
}
