package postgres

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/goravel/framework/contracts/database/driver"
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
		dbColumns []driver.DBColumn
		expected  []driver.Column
	}{
		{
			name: "ValidInput",
			dbColumns: []driver.DBColumn{
				{Name: "id", Type: "int", TypeName: "INT", Nullable: "NO", Extra: "auto_increment", Collation: "utf8_general_ci", Comment: "primary key", Default: "nextval('id_seq'::regclass)"},
				{Name: "name", Type: "varchar", TypeName: "VARCHAR", Nullable: "true", Extra: "", Collation: "utf8_general_ci", Comment: "user name", Default: ""},
			},
			expected: []driver.Column{
				{Autoincrement: true, Collation: "utf8_general_ci", Comment: "primary key", Default: "nextval('id_seq'::regclass)", Name: "id", Nullable: false, Type: "int", TypeName: "INT"},
				{Autoincrement: false, Collation: "utf8_general_ci", Comment: "user name", Default: "", Name: "name", Nullable: true, Type: "varchar", TypeName: "VARCHAR"},
			},
		},
		{
			name:      "EmptyInput",
			dbColumns: []driver.DBColumn{},
		},
		{
			name: "NullableColumn",
			dbColumns: []driver.DBColumn{
				{Name: "description", Type: "text", TypeName: "TEXT", Nullable: "true", Extra: "", Collation: "utf8_general_ci", Comment: "description", Default: ""},
			},
			expected: []driver.Column{
				{Autoincrement: false, Collation: "utf8_general_ci", Comment: "description", Default: "", Name: "description", Nullable: true, Type: "text", TypeName: "TEXT"},
			},
		},
		{
			name: "NonNullableColumn",
			dbColumns: []driver.DBColumn{
				{Name: "created_at", Type: "timestamp", TypeName: "TIMESTAMP", Nullable: "false", Extra: "", Collation: "", Comment: "creation time", Default: "CURRENT_TIMESTAMP"},
			},
			expected: []driver.Column{
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
		dbForeignKeys []driver.DBForeignKey
		expected      []driver.ForeignKey
	}{
		{
			name: "ValidInput",
			dbForeignKeys: []driver.DBForeignKey{
				{Name: "fk_user_id", Columns: "user_id", ForeignSchema: "public", ForeignTable: "users", ForeignColumns: "id", OnUpdate: "c", OnDelete: "r"},
			},
			expected: []driver.ForeignKey{
				{Name: "fk_user_id", Columns: []string{"user_id"}, ForeignSchema: "public", ForeignTable: "users", ForeignColumns: []string{"id"}, OnUpdate: "cascade", OnDelete: "restrict"},
			},
		},
		{
			name:          "EmptyInput",
			dbForeignKeys: []driver.DBForeignKey{},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := s.processor.ProcessForeignKeys(tt.dbForeignKeys)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *ProcessorTestSuite) TestProcessIndexes() {
	tests := []struct {
		name      string
		dbIndexes []driver.DBIndex
		expected  []driver.Index
	}{
		{
			name: "ValidInput",
			dbIndexes: []driver.DBIndex{
				{Name: "users_email_unique", Columns: "email", Type: "BTREE", Primary: false, Unique: true},
				{Name: "PRIMARY", Columns: "id", Type: "BTREE", Primary: true, Unique: true},
				{Name: "users_name_index", Columns: "first_name,last_name", Type: "BTREE", Primary: false, Unique: false},
			},
			expected: []driver.Index{
				{Name: "users_email_unique", Columns: []string{"email"}, Type: "btree", Primary: false, Unique: true},
				{Name: "primary", Columns: []string{"id"}, Type: "btree", Primary: true, Unique: true},
				{Name: "users_name_index", Columns: []string{"first_name", "last_name"}, Type: "btree", Primary: false, Unique: false},
			},
		},
		{
			name:      "EmptyInput",
			dbIndexes: []driver.DBIndex{},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := s.processor.ProcessIndexes(tt.dbIndexes)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *ProcessorTestSuite) TestProcessTypes() {
	// ValidTypes_ReturnsProcessedTypes
	input := []driver.Type{
		{Type: "b", Category: "a"},
		{Type: "c", Category: "b"},
		{Type: "d", Category: "c"},
	}
	expected := []driver.Type{
		{Type: "base", Category: "array"},
		{Type: "composite", Category: "boolean"},
		{Type: "domain", Category: "composite"},
	}

	processor := NewProcessor()
	result := processor.ProcessTypes(input)

	s.Equal(expected, result)

	// UnknownType_ReturnsEmptyString
	input = []driver.Type{
		{Type: "unknown", Category: "a"},
	}
	expected = []driver.Type{
		{Type: "", Category: "array"},
	}

	result = processor.ProcessTypes(input)

	s.Equal(expected, result)

	// UnknownCategory_ReturnsEmptyString
	input = []driver.Type{
		{Type: "b", Category: "unknown"},
	}
	expected = []driver.Type{
		{Type: "base", Category: ""},
	}

	result = processor.ProcessTypes(input)

	s.Equal(expected, result)

	// EmptyInput_ReturnsEmptyOutput
	input = []driver.Type{}
	expected = []driver.Type{}

	result = processor.ProcessTypes(input)

	s.Equal(expected, result)
}
