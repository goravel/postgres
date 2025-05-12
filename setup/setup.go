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
        "timezone": "UTC", // Asia/Shanghai
        "singular": false,
        "prefix":   "",
        "via": func() (driver.Driver, error) {
            return postgresfacades.Postgres("postgres")
        },
        // Optional
        "schema":   config.Env("DB_SCHEMA", "public"),
    }`

func main() {
	packages.Setup(os.Args).
		Install(
			modify.GoFile(path.Config("app.go")).
				Find(match.Imports()).Modify(modify.AddImport(packages.GetModulePath())).
				Find(match.Providers()).Modify(modify.Register("&postgres.ServiceProvider{}", "&queue.ServiceProvider{}")),
			modify.GoFile(path.Config("database.go")).
				Find(match.Imports()).Modify(modify.AddImport("github.com/goravel/framework/contracts/database/driver"), modify.AddImport("github.com/goravel/postgres/facades", "postgresfacades")).
				Find(match.Config("database.connections")).Modify(modify.AddConfig("postgres", config)),
		).
		Uninstall(
			modify.GoFile(path.Config("app.go")).
				Find(match.Providers()).Modify(modify.Unregister("&postgres.ServiceProvider{}")).
				Find(match.Imports()).Modify(modify.RemoveImport(packages.GetModulePath())),
			modify.GoFile(path.Config("database.go")).
				Find(match.Config("database.connections")).Modify(modify.RemoveConfig("postgres")).
				Find(match.Imports()).Modify(modify.RemoveImport("github.com/goravel/framework/contracts/database/driver"), modify.RemoveImport("github.com/goravel/postgres/facades", "postgresfacades")),
		).
		Execute()
}
