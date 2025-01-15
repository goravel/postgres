package postgres

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/goravel/framework/contracts/database/schema"
)

type ProcessorTestSuite struct {
	suite.Suite
	processor *Processor
}

func TestProcessorTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessorTestSuite))
}

func (s *ProcessorTestSuite) SetupTest() {
	s.processor = NewProcessor()
}

func (s *ProcessorTestSuite) TestProcessColumns() {
	tests := []struct {
		name      string
		dbColumns []schema.DBColumn
		expected  []schema.Column
	}{
		{
			name: "ValidInput",
			dbColumns: []schema.DBColumn{
				{Name: "id", Type: "int", TypeName: "INT", Nullable: "NO", Extra: "auto_increment", Collation: "utf8_general_ci", Comment: "primary key", Default: "nextval('id_seq'::regclass)"},
				{Name: "name", Type: "varchar", TypeName: "VARCHAR", Nullable: "true", Extra: "", Collation: "utf8_general_ci", Comment: "user name", Default: ""},
			},
			expected: []schema.Column{
				{Autoincrement: true, Collation: "utf8_general_ci", Comment: "primary key", Default: "nextval('id_seq'::regclass)", Name: "id", Nullable: false, Type: "int", TypeName: "INT"},
				{Autoincrement: false, Collation: "utf8_general_ci", Comment: "user name", Default: "", Name: "name", Nullable: true, Type: "varchar", TypeName: "VARCHAR"},
			},
		},
		{
			name:      "EmptyInput",
			dbColumns: []schema.DBColumn{},
		},
		{
			name: "NullableColumn",
			dbColumns: []schema.DBColumn{
				{Name: "description", Type: "text", TypeName: "TEXT", Nullable: "true", Extra: "", Collation: "utf8_general_ci", Comment: "description", Default: ""},
			},
			expected: []schema.Column{
				{Autoincrement: false, Collation: "utf8_general_ci", Comment: "description", Default: "", Name: "description", Nullable: true, Type: "text", TypeName: "TEXT"},
			},
		},
		{
			name: "NonNullableColumn",
			dbColumns: []schema.DBColumn{
				{Name: "created_at", Type: "timestamp", TypeName: "TIMESTAMP", Nullable: "false", Extra: "", Collation: "", Comment: "creation time", Default: "CURRENT_TIMESTAMP"},
			},
			expected: []schema.Column{
				{Autoincrement: false, Collation: "", Comment: "creation time", Default: "CURRENT_TIMESTAMP", Name: "created_at", Nullable: false, Type: "timestamp", TypeName: "TIMESTAMP"},
			},
		},
	}

	processor := NewProcessor()
	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := processor.ProcessColumns(tt.dbColumns)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *ProcessorTestSuite) TestProcessForeignKeys() {
	tests := []struct {
		name          string
		dbForeignKeys []schema.DBForeignKey
		expected      []schema.ForeignKey
	}{
		{
			name: "ValidInput",
			dbForeignKeys: []schema.DBForeignKey{
				{Name: "fk_user_id", Columns: "user_id", ForeignSchema: "public", ForeignTable: "users", ForeignColumns: "id", OnUpdate: "c", OnDelete: "r"},
			},
			expected: []schema.ForeignKey{
				{Name: "fk_user_id", Columns: []string{"user_id"}, ForeignSchema: "public", ForeignTable: "users", ForeignColumns: []string{"id"}, OnUpdate: "cascade", OnDelete: "restrict"},
			},
		},
		{
			name:          "EmptyInput",
			dbForeignKeys: []schema.DBForeignKey{},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := s.processor.ProcessForeignKeys(tt.dbForeignKeys)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *ProcessorTestSuite) TestProcessTypes() {
	// ValidTypes_ReturnsProcessedTypes
	input := []schema.Type{
		{Type: "b", Category: "a"},
		{Type: "c", Category: "b"},
		{Type: "d", Category: "c"},
	}
	expected := []schema.Type{
		{Type: "base", Category: "array"},
		{Type: "composite", Category: "boolean"},
		{Type: "domain", Category: "composite"},
	}

	processor := NewProcessor()
	result := processor.ProcessTypes(input)

	s.Equal(expected, result)

	// UnknownType_ReturnsEmptyString
	input = []schema.Type{
		{Type: "unknown", Category: "a"},
	}
	expected = []schema.Type{
		{Type: "", Category: "array"},
	}

	result = processor.ProcessTypes(input)

	s.Equal(expected, result)

	// UnknownCategory_ReturnsEmptyString
	input = []schema.Type{
		{Type: "b", Category: "unknown"},
	}
	expected = []schema.Type{
		{Type: "base", Category: ""},
	}

	result = processor.ProcessTypes(input)

	s.Equal(expected, result)

	// EmptyInput_ReturnsEmptyOutput
	input = []schema.Type{}
	expected = []schema.Type{}

	result = processor.ProcessTypes(input)

	s.Equal(expected, result)
}
