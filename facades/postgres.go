package facades

import (
	"log"

	"github.com/goravel/framework/contracts/database/driver"

	"github.com/goravel/postgres"
)

func Postgres(connection string) driver.Driver {
	if postgres.App == nil {
		log.Fatalln("please register postgres service provider")
		return nil
	}

	instance, err := postgres.App.MakeWith(postgres.Binding, map[string]any{
		"connection": connection,
	})
	if err != nil {
		log.Fatalln(err)
		return nil
	}

	return instance.(*postgres.Postgres)
}
