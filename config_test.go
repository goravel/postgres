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
	config     *ConfigBuilder
	connection string
	mockConfig *mocksconfig.Config
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, &ConfigTestSuite{
		connection: "mysql",
	})
}

func (s *ConfigTestSuite) SetupTest() {
	s.mockConfig = mocksconfig.NewConfig(s.T())
	s.config = NewConfigBuilder(s.mockConfig, s.connection)
}

func (s *ConfigTestSuite) TestReads() {
	database := "forge"
	prefix := "goravel_"
	singular := false

	// Test when configs is empty
	s.mockConfig.EXPECT().Get("database.connections.mysql.read").Return(nil).Once()
	s.Nil(s.config.Reads())

	// Test when configs is not empty
	s.mockConfig.EXPECT().Get("database.connections.mysql.read").Return([]contracts.Config{
		{
			Database: database,
		},
	}).Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return(prefix).Once()
	s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(singular).Once()
	s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(false).Once()
	s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nil).Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.driver", s.connection)).Return("postgres").Once()

	s.Equal([]contracts.FullConfig{
		{
			Connection: s.connection,
			Driver:     "postgres",
			Prefix:     prefix,
			Config: contracts.Config{
				Database: database,
			},
		},
	}, s.config.Reads())
}

func (s *ConfigTestSuite) TestWrites() {
	database := "forge"
	prefix := "goravel_"
	singular := false

	// Test when configBuilder is empty
	s.mockConfig.EXPECT().Get("database.connections.mysql.write").Return(nil).Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.driver", s.connection)).Return("postgres").Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.database", s.connection)).Return(database).Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return(prefix).Once()
	s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(singular).Once()
	s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(false).Once()
	s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nil).Once()

	s.Equal([]contracts.FullConfig{
		{
			Connection: s.connection,
			Driver:     "postgres",
			Prefix:     prefix,
			Config: contracts.Config{
				Database: database,
			},
		},
	}, s.config.Writes())

	// Test when configBuilder is not empty
	s.mockConfig.EXPECT().Get("database.connections.mysql.write").Return([]contracts.Config{
		{
			Database: database,
		},
	}).Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.driver", s.connection)).Return("postgres").Once()
	s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return(prefix).Once()
	s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(singular).Once()
	s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(false).Once()
	s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nil).Once()

	s.Equal([]contracts.FullConfig{
		{
			Connection: s.connection,
			Driver:     "postgres",
			Prefix:     prefix,
			Config: contracts.Config{
				Database: database,
			},
		},
	}, s.config.Writes())
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
	charset := "utf8mb4"
	loc := "Local"
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
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.driver", s.connection)).Return("postgres").Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.dsn", s.connection)).Return(dsn).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.host", s.connection)).Return(host).Once()
				s.mockConfig.EXPECT().GetInt(fmt.Sprintf("database.connections.%s.port", s.connection)).Return(port).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.database", s.connection)).Return(database).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.username", s.connection)).Return(username).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.password", s.connection)).Return(password).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.charset", s.connection)).Return(charset).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.loc", s.connection)).Return(loc).Once()
			},
			expectConfigs: []contracts.FullConfig{
				{
					Connection:   s.connection,
					Driver:       "postgres",
					Prefix:       prefix,
					Singular:     singular,
					Charset:      charset,
					Loc:          loc,
					NoLowerCase:  true,
					NameReplacer: nameReplacer,
					Config: contracts.Config{
						Dsn:      dsn,
						Host:     host,
						Port:     port,
						Database: database,
						Username: username,
						Password: password,
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
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.driver", s.connection)).Return("postgres").Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.prefix", s.connection)).Return(prefix).Once()
				s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.singular", s.connection)).Return(singular).Once()
				s.mockConfig.EXPECT().GetBool(fmt.Sprintf("database.connections.%s.no_lower_case", s.connection)).Return(true).Once()
				s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.name_replacer", s.connection)).Return(nameReplacer).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.charset", s.connection)).Return(charset).Once()
				s.mockConfig.EXPECT().GetString(fmt.Sprintf("database.connections.%s.loc", s.connection)).Return(loc).Once()
			},
			expectConfigs: []contracts.FullConfig{
				{
					Connection:   s.connection,
					Driver:       "postgres",
					Prefix:       prefix,
					Singular:     singular,
					Charset:      charset,
					Loc:          loc,
					NoLowerCase:  true,
					NameReplacer: nameReplacer,
					Config: contracts.Config{
						Dsn:      dsn,
						Database: database,
						Host:     host,
						Port:     port,
						Username: username,
						Password: password,
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
