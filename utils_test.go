package postgres

import (
	"fmt"
	"testing"

	contractstesting "github.com/goravel/framework/contracts/testing"
	"github.com/goravel/framework/errors"
	mocksschema "github.com/goravel/framework/mocks/database/schema"
	"github.com/goravel/framework/support/env"
	"github.com/stretchr/testify/assert"
)

func TestGetDefaultValue(t *testing.T) {
	def := true
	assert.Equal(t, "'1'", getDefaultValue(def))

	def = false
	assert.Equal(t, "'0'", getDefaultValue(def))

	defInt := 123
	assert.Equal(t, "'123'", getDefaultValue(defInt))

	defString := "abc"
	assert.Equal(t, "'abc'", getDefaultValue(defString))

	defExpression := Expression("abc")
	assert.Equal(t, "abc", getDefaultValue(defExpression))
}

func TestGetExposedPort(t *testing.T) {
	assert.Equal(t, 1, getExposedPort([]string{"1:2"}, 2))
}

func TestGetType(t *testing.T) {
	// valid type
	mockColumn := mocksschema.NewColumnDefinition(t)
	mockColumn.EXPECT().GetType().Return("string").Once()

	mockGrammar := mocksschema.NewGrammar(t)
	mockGrammar.EXPECT().TypeString(mockColumn).Return("varchar").Once()

	assert.Equal(t, "varchar", getType(mockGrammar, mockColumn))

	// invalid type
	mockColumn1 := mocksschema.NewColumnDefinition(t)
	mockColumn1.EXPECT().GetType().Return("invalid").Once()

	mockGrammar1 := mocksschema.NewGrammar(t)

	assert.Empty(t, getType(mockGrammar1, mockColumn1))
}

func TestGetValidPort(t *testing.T) {
	assert.True(t, getValidPort() > 0)
}

func TestParseSchemaAndTable(t *testing.T) {
	tests := []struct {
		reference      string
		defaultSchema  string
		expectedSchema string
		expectedTable  string
		expectedError  error
	}{
		{"public.users", "public", "public", "users", nil},
		{"users", "goravel", "goravel", "users", nil},
		{"", "", "", "", errors.SchemaEmptyReferenceString},
		{"public.users.extra", "", "", "", errors.SchemaErrorReferenceFormat},
	}

	for _, test := range tests {
		schema, table, err := parseSchemaAndTable(test.reference, test.defaultSchema)
		assert.Equal(t, test.expectedSchema, schema)
		assert.Equal(t, test.expectedTable, table)
		assert.Equal(t, test.expectedError, err)
	}
}

func TestImageToCommand(t *testing.T) {
	command, exposedPorts := imageToCommand(nil)
	assert.Equal(t, "", command)
	assert.Nil(t, exposedPorts)

	command, exposedPorts = imageToCommand(&contractstesting.Image{
		Repository: "redis",
		Tag:        "latest",
	})

	assert.Equal(t, "docker run --rm -d redis:latest", command)
	assert.Nil(t, exposedPorts)

	command, exposedPorts = imageToCommand(&contractstesting.Image{
		Repository:   "redis",
		Tag:          "latest",
		ExposedPorts: []string{"6379"},
		Env:          []string{"a=b"},
	})
	assert.Equal(t, fmt.Sprintf("docker run --rm -d -e a=b -p %d:6379 redis:latest", getExposedPort(exposedPorts, 6379)), command)
	assert.True(t, getExposedPort(exposedPorts, 6379) > 0)

	command, exposedPorts = imageToCommand(&contractstesting.Image{
		Repository:   "redis",
		Tag:          "latest",
		ExposedPorts: []string{"1234:6379"},
		Env:          []string{"a=b"},
	})
	assert.Equal(t, "docker run --rm -d -e a=b -p 1234:6379 redis:latest", command)
	assert.Equal(t, []string{"1234:6379"}, exposedPorts)
}

func TestRun(t *testing.T) {
	if env.IsWindows() {
		t.Skip("Skip test that using Docker")
	}

	_, err := run("ls")
	assert.Nil(t, err)
}
