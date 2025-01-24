package postgres

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	mocksconfig "github.com/goravel/framework/mocks/config"
	"github.com/goravel/postgres/contracts"
)

type ConfigTestSuite struct {
	suite.Suite
	config     *Config
	connection string
	mockConfig *mocksconfig.Config
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, &ConfigTestSuite{
		connection: "postgres",
	})
}

func (s *ConfigTestSuite) SetupTest() {
	s.mockConfig = mocksconfig.NewConfig(s.T())
	s.config = NewConfig(s.mockConfig, s.connection)
}

func (s *ConfigTestSuite) TestReads() {
	// Test when configs is empty
	s.mockConfig.EXPECT().Get("database.connections.postgres.read").Return(nil).Once()
	s.Nil(s.config.Reads())

	// Test when configs is not empty
	s.mockConfig.EXPECT().Get("database.connections.postgres.read").Return([]contracts.Config{
		{
			Dsn:      "dsn",
			Database: "forge",
			Host:     "localhost",
			Port:     3306,
			Username: "root",
			Password: "123123",
			Schema:   "public",
		},
	}).Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return("goravel_").Once()
	s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(false).Once()
	s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(false).Once()
	s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nil).Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.sslmode", s.connection)).Return("disable").Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.timezone", s.connection)).Return("UTC").Once()
	s.Equal([]contracts.FullConfig{
		{
			Connection:   s.connection,
			Driver:       Name,
			Prefix:       "goravel_",
			Singular:     false,
			Sslmode:      "disable",
			Timezone:     "UTC",
			NoLowerCase:  false,
			NameReplacer: nil,
			Config: contracts.Config{
				Dsn:      "dsn",
				Database: "forge",
				Host:     "localhost",
				Port:     3306,
				Username: "root",
				Password: "123123",
				Schema:   "public",
			},
		},
	}, s.config.Reads())
}

func (s *ConfigTestSuite) TestWrites() {
	s.Run("success when configs is empty", func() {
		s.mockConfig.EXPECT().Get("database.connections.postgres.write").Return(nil).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return("goravel_").Once()
		s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(false).Once()
		s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(false).Once()
		s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nil).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.sslmode", s.connection)).Return("disable").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.timezone", s.connection)).Return("UTC").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.schema", s.connection), "public").Return("public").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.dsn", s.connection)).Return("dsn").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.host", s.connection)).Return("localhost").Once()
		s.mockConfig.EXPECT().GetInt(fmt.Sprintf("database.connections.%s.port", s.connection)).Return(3306).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.username", s.connection)).Return("root").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.password", s.connection)).Return("123123").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.database", s.connection)).Return("forge").Once()

		s.Equal([]contracts.FullConfig{
			{
				Connection:   s.connection,
				Driver:       Name,
				Prefix:       "goravel_",
				Singular:     false,
				Sslmode:      "disable",
				Timezone:     "UTC",
				NoLowerCase:  false,
				NameReplacer: nil,
				Config: contracts.Config{
					Dsn:      "dsn",
					Database: "forge",
					Host:     "localhost",
					Port:     3306,
					Username: "root",
					Password: "123123",
					Schema:   "public",
				},
			},
		}, s.config.Writes())
	})

	s.Run("success when configs is not empty", func() {
		s.mockConfig.EXPECT().Get("database.connections.postgres.write").Return([]contracts.Config{
			{
				Database: "forge",
			},
		}).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return("goravel_").Once()
		s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(false).Once()
		s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(false).Once()
		s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nil).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.sslmode", s.connection)).Return("disable").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.timezone", s.connection)).Return("UTC").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.schema", s.connection), "public").Return("public").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.dsn", s.connection)).Return("dsn").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.host", s.connection)).Return("localhost").Once()
		s.mockConfig.EXPECT().GetInt(fmt.Sprintf("database.connections.%s.port", s.connection)).Return(3306).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.username", s.connection)).Return("root").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.password", s.connection)).Return("123123").Once()

		s.Equal([]contracts.FullConfig{
			{
				Connection:   s.connection,
				Driver:       "postgres",
				Prefix:       "goravel_",
				Singular:     false,
				Sslmode:      "disable",
				Timezone:     "UTC",
				NoLowerCase:  false,
				NameReplacer: nil,
				Config: contracts.Config{
					Dsn:      "dsn",
					Database: "forge",
					Host:     "localhost",
					Port:     3306,
					Username: "root",
					Password: "123123",
					Schema:   "public",
				},
			},
		}, s.config.Writes())
	})
}

func (s *ConfigTestSuite) TestFillDefault() {
	dsn := "dsn"
	host := "localhost"
	port := 3306
	database := "forge"
	username := "root"
	password := "123123"
	prefix := "goravel_"
	singular := false
	sslmode := "disable"
	timezone := "UTC"
	schema := "public"
	nameReplacer := strings.NewReplacer("a", "b")

	tests := []struct {
		name          string
		configs       []contracts.Config
		setup         func()
		expectConfigs []contracts.FullConfig
	}{
		{
			name:    "success when configs is empty",
			setup:   func() {},
			configs: []contracts.Config{},
		},
		{
			name:    "success when configs have item but key is empty",
			configs: []contracts.Config{{}},
			setup: func() {
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return(prefix).Once()
				s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(singular).Once()
				s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(true).Once()
				s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nameReplacer).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.dsn", s.connection)).Return(dsn).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.host", s.connection)).Return(host).Once()
				s.mockConfig.EXPECT().GetInt(fmt.Sprintf("database.connections.%s.port", s.connection)).Return(port).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.database", s.connection)).Return(database).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.username", s.connection)).Return(username).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.password", s.connection)).Return(password).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.sslmode", s.connection)).Return(sslmode).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.timezone", s.connection)).Return(timezone).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.schema", s.connection), "public").Return(schema).Once()
			},
			expectConfigs: []contracts.FullConfig{
				{
					Connection:   s.connection,
					Driver:       "postgres",
					Prefix:       prefix,
					Singular:     singular,
					Sslmode:      sslmode,
					Timezone:     timezone,
					NoLowerCase:  true,
					NameReplacer: nameReplacer,
					Config: contracts.Config{
						Dsn:      dsn,
						Host:     host,
						Port:     port,
						Database: database,
						Username: username,
						Password: password,
						Schema:   schema,
					},
				},
			},
		},
		{
			name: "success when configs have item",
			configs: []contracts.Config{
				{
					Dsn:      dsn,
					Host:     host,
					Port:     port,
					Database: database,
					Username: username,
					Password: password,
				},
			},
			setup: func() {
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return(prefix).Once()
				s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(singular).Once()
				s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(true).Once()
				s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nameReplacer).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.sslmode", s.connection)).Return(sslmode).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.timezone", s.connection)).Return(timezone).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.schema", s.connection), "public").Return(schema).Once()
			},
			expectConfigs: []contracts.FullConfig{
				{
					Connection:   s.connection,
					Driver:       "postgres",
					Prefix:       prefix,
					Singular:     singular,
					Sslmode:      sslmode,
					Timezone:     timezone,
					NoLowerCase:  true,
					NameReplacer: nameReplacer,
					Config: contracts.Config{
						Dsn:      dsn,
						Database: database,
						Host:     host,
						Port:     port,
						Username: username,
						Password: password,
						Schema:   schema,
					},
				},
			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			test.setup()
			configs := s.config.fillDefault(test.configs)

			s.Equal(test.expectConfigs, configs)
		})
	}
}
