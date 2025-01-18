package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/postgres"
)

func TestDriverName(t *testing.T) {
	// framework will judge the driver name in buildLockForUpdate and buildSharedLock methods,
	// we need to ensure that postgres.Dialector{}.Name() is not modified suddenly.
	assert.Equal(t, Name, postgres.Dialector{}.Name())
}
