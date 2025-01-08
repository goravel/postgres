package postgres

import (
	"fmt"

	contractsconfig "github.com/goravel/framework/contracts/config"
	"github.com/goravel/framework/contracts/database"

	"github.com/goravel/postgres/contracts"
)

type ConfigBuilder struct {
	config     contractsconfig.Config
	connection string
}

func NewConfigBuilder(config contractsconfig.Config, connection string) *ConfigBuilder {
	return &ConfigBuilder{
		config:     config,
		connection: connection,
	}
}

func (c *ConfigBuilder) Config() contractsconfig.Config {
	return c.config
}

func (c *ConfigBuilder) Connection() string {
	return c.connection
}

func (c *ConfigBuilder) Reads() []contracts.FullConfig {
	configs := c.config.Get(fmt.Sprintf("database.connections.%s.read", c.connection))
	if readConfigs, ok := configs.([]contracts.Config); ok {
		return c.fillDefault(readConfigs)
	}

	return nil
}

func (c *ConfigBuilder) Writes() []contracts.FullConfig {
	configs := c.config.Get(fmt.Sprintf("database.connections.%s.write", c.connection))
	if writeConfigs, ok := configs.([]contracts.Config); ok {
		return c.fillDefault(writeConfigs)
	}

	// Use default db configuration when write is empty
	return c.fillDefault([]contracts.Config{{}})
}

func (c *ConfigBuilder) fillDefault(configs []contracts.Config) []contracts.FullConfig {
	if len(configs) == 0 {
		return nil
	}

	var fullConfigs []contracts.FullConfig
	driver := c.config.GetString(fmt.Sprintf("database.connections.%s.driver", c.connection))

	for _, config := range configs {
		fullConfig := contracts.FullConfig{
			Config:      config,
			Connection:  c.connection,
			Driver:      driver,
			Prefix:      c.config.GetString(fmt.Sprintf("database.connections.%s.prefix", c.connection)),
			Singular:    c.config.GetBool(fmt.Sprintf("database.connections.%s.singular", c.connection)),
			NoLowerCase: c.config.GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", c.connection)),
			Sslmode:     c.config.GetString(fmt.Sprintf("database.connections.%s.sslmode", c.connection)),
			Timezone:    c.config.GetString(fmt.Sprintf("database.connections.%s.timezone", c.connection)),
		}
		if nameReplacer := c.config.Get(fmt.Sprintf("database.connections.%s.name_replacer", c.connection)); nameReplacer != nil {
			if replacer, ok := nameReplacer.(database.Replacer); ok {
				fullConfig.NameReplacer = replacer
			}
		}
		if fullConfig.Dsn == "" {
			fullConfig.Dsn = c.config.GetString(fmt.Sprintf("database.connections.%s.dsn", c.connection))
		}
		if fullConfig.Host == "" {
			fullConfig.Host = c.config.GetString(fmt.Sprintf("database.connections.%s.host", c.connection))
		}
		if fullConfig.Port == 0 {
			fullConfig.Port = c.config.GetInt(fmt.Sprintf("database.connections.%s.port", c.connection))
		}
		if fullConfig.Username == "" {
			fullConfig.Username = c.config.GetString(fmt.Sprintf("database.connections.%s.username", c.connection))
		}
		if fullConfig.Password == "" {
			fullConfig.Password = c.config.GetString(fmt.Sprintf("database.connections.%s.password", c.connection))
		}
		if fullConfig.Schema == "" {
			fullConfig.Schema = c.config.GetString(fmt.Sprintf("database.connections.%s.schema", c.connection), "public")
		}
		if config.Database == "" {
			fullConfig.Database = c.config.GetString(fmt.Sprintf("database.connections.%s.database", c.connection))
		}
		fullConfigs = append(fullConfigs, fullConfig)
	}

	return fullConfigs
}
