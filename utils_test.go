package postgres

import (
	"testing"

	"github.com/goravel/framework/errors"
	mocksschema "github.com/goravel/framework/mocks/database/schema"
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
