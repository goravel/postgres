package postgres

import (
	"testing"

	mocksconfig "github.com/goravel/framework/mocks/config"
	"github.com/goravel/framework/testing/utils"
	"github.com/goravel/postgres/contracts"
	mocks "github.com/goravel/postgres/mocks"
	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	writes := []contracts.FullConfig{
		{
			Config: contracts.Config{
				Host:     "localhost",
				Database: "goravel",
				Username: "goravel",
				Password: "Framework!123",
				Schema:   "public",
			},
			Timezone: "UTC",
		},
	}

	docker := NewDocker(nil, writes[0].Database, writes[0].Username, writes[0].Password)
	assert.NoError(t, docker.Build())

	writes[0].Config.Port = docker.port
	_, err := docker.connect()
	assert.NoError(t, err)

	mockConfig := mocks.NewConfigBuilder(t)
	mockConfigFacade := mocksconfig.NewConfig(t)

	// instance
	mockConfig.EXPECT().Writes().Return(writes).Once()
	mockConfig.EXPECT().Reads().Return([]contracts.FullConfig{}).Once()

	// gormConfig
	mockConfig.EXPECT().Config().Return(mockConfigFacade).Once()
	mockConfigFacade.EXPECT().GetBool("app.debug").Return(true).Once()
	mockConfigFacade.EXPECT().GetInt("database.slow_threshold", 200).Return(200).Once()
	mockConfig.EXPECT().Writes().Return(writes).Once()

	// configurePool
	mockConfigFacade.EXPECT().GetInt("database.pool.max_idle_conns", 10).Return(10).Once()
	mockConfigFacade.EXPECT().GetInt("database.pool.max_open_conns", 100).Return(100).Once()
	mockConfigFacade.EXPECT().GetInt("database.pool.conn_max_idletime", 3600).Return(3600).Once()
	mockConfigFacade.EXPECT().GetInt("database.pool.conn_max_lifetime", 3600).Return(3600).Once()
	mockConfig.EXPECT().Config().Return(mockConfigFacade).Once()

	// configureReadWriteSeparate
	mockConfig.EXPECT().Writes().Return(writes).Once()
	mockConfig.EXPECT().Reads().Return([]contracts.FullConfig{}).Once()

	postgres := &Postgres{
		config: mockConfig,
		log:    utils.NewTestLog(),
	}
	version := postgres.getVersion()
	assert.Contains(t, version, "Debian")
	assert.NoError(t, docker.Shutdown())
}
