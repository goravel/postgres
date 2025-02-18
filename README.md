# Postgres

The Postgres driver for facades.Orm() of Goravel.

## Version

| goravel/postgres | goravel/framework |
|------------------|-------------------|
| v1.0.*          | v1.16.*           |

## Install

1. Add package

```
go get -u github.com/goravel/postgres
```

2. Register service provider

```
// config/app.go
import "github.com/goravel/postgres"

"providers": []foundation.ServiceProvider{
    ...
    &postgres.ServiceProvider{},
}
```

3. Add postgres driver to `config/database.go` file

```
// config/database.go
import (
    "github.com/goravel/framework/contracts/database/driver"
    "github.com/goravel/postgres/contracts"
    postgresfacades "github.com/goravel/postgres/facades"
)

"connections": map[string]any{
    ...
    "postgres": map[string]any{
        "host":     config.Env("DB_HOST", "127.0.0.1"),
        "port":     config.Env("DB_PORT", 5432),
        "database": config.Env("DB_DATABASE", "forge"),
        "username": config.Env("DB_USERNAME", ""),
        "password": config.Env("DB_PASSWORD", ""),
        "sslmode":  "disable",
        "timezone": "UTC", // Asia/Shanghai
        "singular": false,
        "prefix":   "",
        "schema":   "",
        "via": func() (driver.Driver, error) {
            return postgresfacades.Postgres("postgres")
        },
        // Optional
        "read": []contracts.Config{
            {Host: "192.168.1.1", Port: 3306, Database: "forge", Username: "root", Password: "123123"},
        },
        // Optional
        "write": []contracts.Config{
            {Host: "192.168.1.2", Port: 3306, Database: "forge", Username: "root", Password: "123123"},
        },
    },
}
```
