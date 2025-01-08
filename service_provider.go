package postgres

import (
	"github.com/goravel/framework/contracts/foundation"
)

const (
	Binding    = "goravel.postgres"
	DriverName = "postgres"
)

var App foundation.Application

type ServiceProvider struct {
}

func (receiver *ServiceProvider) Register(app foundation.Application) {
	App = app

	app.BindWith(Binding, func(app foundation.Application, parameters map[string]any) (any, error) {
		return NewPostgres(NewConfigBuilder(app.MakeConfig(), parameters["connection"].(string)), app.MakeLog()), nil
	})
}

func (receiver *ServiceProvider) Boot(app foundation.Application) {

}
