package postgres

import (
	"testing"

	contractsdriver "github.com/goravel/framework/contracts/database/driver"
	databasedb "github.com/goravel/framework/database/db"
	"github.com/goravel/framework/database/schema"
	"github.com/goravel/framework/errors"
	"github.com/goravel/framework/foundation/json"
	mocksdriver "github.com/goravel/framework/mocks/database/driver"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	"github.com/goravel/framework/support/convert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type GrammarSuite struct {
	suite.Suite
	grammar *Grammar
}

func TestGrammarSuite(t *testing.T) {
	suite.Run(t, &GrammarSuite{})
}

func (s *GrammarSuite) SetupTest() {
	s.grammar = NewGrammar("goravel_")
}

func (s *GrammarSuite) TestCompileAdd() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockColumn := mocksdriver.NewColumnDefinition(s.T())

	mockBlueprint.EXPECT().GetTableName().Return("users").Once()
	mockColumn.EXPECT().GetName().Return("name").Once()
	mockColumn.EXPECT().GetType().Return("string").Twice()
	mockColumn.EXPECT().GetDefault().Return("goravel").Twice()
	mockColumn.EXPECT().GetNullable().Return(false).Once()
	mockColumn.EXPECT().GetLength().Return(1).Once()
	mockColumn.EXPECT().IsChange().Return(false).Times(4)
	mockColumn.EXPECT().IsSetGeneratedAs().Return(false).Twice()
	mockBlueprint.EXPECT().HasCommand("primary").Return(false).Once()

	sql := s.grammar.CompileAdd(mockBlueprint, &contractsdriver.Command{
		Column: mockColumn,
	})

	s.Equal(`alter table "goravel_users" add column "name" varchar(1) default 'goravel' not null`, sql)
}

func (s *GrammarSuite) TestCompileChange() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockColumn := mocksdriver.NewColumnDefinition(s.T())

	mockBlueprint.EXPECT().GetTableName().Return("users").Once()
	mockColumn.EXPECT().GetName().Return("name").Times(3)
	mockColumn.EXPECT().GetType().Return("string").Once()
	mockColumn.EXPECT().GetDefault().Return("goravel").Twice()
	mockColumn.EXPECT().GetNullable().Return(false).Once()
	mockColumn.EXPECT().GetLength().Return(1).Once()
	mockColumn.EXPECT().IsChange().Return(true).Times(4)
	mockColumn.EXPECT().IsSetGeneratedAs().Return(false).Times(3)
	mockColumn.EXPECT().GetAutoIncrement().Return(false).Once()

	sql := s.grammar.CompileChange(mockBlueprint, &contractsdriver.Command{
		Column: mockColumn,
	})

	s.Equal([]string{
		`alter table "goravel_users" alter column "name" type varchar(1), alter column "name" set default 'goravel', alter column "name" set not null`,
	}, sql)
}

func (s *GrammarSuite) TestCompileColumns() {
	tests := []struct {
		name          string
		schema        string
		table         string
		expectedSQL   string
		expectedError error
	}{
		{
			name:   "with schema and table",
			schema: "public",
			table:  "users",
			expectedSQL: `select a.attname as name, t.typname as type_name, format_type(a.atttypid, a.atttypmod) as type, ` +
				`(select tc.collcollate from pg_catalog.pg_collation tc where tc.oid = a.attcollation) as collation, ` +
				`not a.attnotnull as nullable, ` +
				`(select pg_get_expr(adbin, adrelid) from pg_attrdef where c.oid = pg_attrdef.adrelid and pg_attrdef.adnum = a.attnum) as default, ` +
				`col_description(c.oid, a.attnum) as comment ` +
				`from pg_attribute a, pg_class c, pg_type t, pg_namespace n ` +
				`where c.relname = 'goravel_users' and n.nspname = 'public' and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid and n.oid = c.relnamespace ` +
				`order by a.attnum`,
			expectedError: nil,
		},
		{
			name:   "with table containing dots",
			schema: "public",
			table:  "schema.users",
			expectedSQL: `select a.attname as name, t.typname as type_name, format_type(a.atttypid, a.atttypmod) as type, ` +
				`(select tc.collcollate from pg_catalog.pg_collation tc where tc.oid = a.attcollation) as collation, ` +
				`not a.attnotnull as nullable, ` +
				`(select pg_get_expr(adbin, adrelid) from pg_attrdef where c.oid = pg_attrdef.adrelid and pg_attrdef.adnum = a.attnum) as default, ` +
				`col_description(c.oid, a.attnum) as comment ` +
				`from pg_attribute a, pg_class c, pg_type t, pg_namespace n ` +
				`where c.relname = 'goravel_users' and n.nspname = 'schema' and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid and n.oid = c.relnamespace ` +
				`order by a.attnum`,
			expectedError: nil,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			sql, err := s.grammar.CompileColumns(test.schema, test.table)
			if test.expectedError != nil {
				s.Equal(test.expectedError.Error(), err.Error())
			} else {
				s.Nil(err)
				s.Equal(test.expectedSQL, sql)
			}
		})
	}
}

func (s *GrammarSuite) TestCompileComment() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockColumnDefinition := mocksdriver.NewColumnDefinition(s.T())
	mockBlueprint.On("GetTableName").Return("users").Once()
	mockColumnDefinition.On("GetName").Return("id").Once()
	mockColumnDefinition.On("IsSetComment").Return(true).Once()
	mockColumnDefinition.On("GetComment").Return("comment").Once()

	sql := s.grammar.CompileComment(mockBlueprint, &contractsdriver.Command{
		Column: mockColumnDefinition,
	})

	s.Equal(`comment on column "goravel_users"."id" is 'comment'`, sql)
}

func (s *GrammarSuite) TestCompileCreate() {
	mockColumn1 := mocksdriver.NewColumnDefinition(s.T())
	mockColumn2 := mocksdriver.NewColumnDefinition(s.T())
	mockBlueprint := mocksdriver.NewBlueprint(s.T())

	// postgres.go::CompileCreate
	mockBlueprint.EXPECT().GetTableName().Return("users").Once()
	// utils.go::getColumns
	mockBlueprint.EXPECT().GetAddedColumns().Return([]contractsdriver.ColumnDefinition{
		mockColumn1, mockColumn2,
	}).Once()
	// utils.go::getColumns
	mockColumn1.EXPECT().GetName().Return("id").Once()
	// utils.go::getType
	mockColumn1.EXPECT().GetType().Return("integer").Once()
	// postgres.go::TypeInteger
	mockColumn1.EXPECT().GetAutoIncrement().Return(true).Once()
	// postgres.go::ModifyDefault
	mockColumn1.EXPECT().GetDefault().Return(nil).Once()
	// postgres.go::ModifyIncrement
	mockBlueprint.EXPECT().HasCommand("primary").Return(false).Once()
	mockColumn1.EXPECT().GetType().Return("integer").Once()
	mockColumn1.EXPECT().GetAutoIncrement().Return(true).Once()
	// postgres.go::ModifyNullable
	mockColumn1.EXPECT().GetNullable().Return(false).Once()
	mockColumn1.EXPECT().IsChange().Return(false).Times(5)
	mockColumn1.EXPECT().IsSetGeneratedAs().Return(false).Twice()

	// utils.go::getColumns
	mockColumn2.EXPECT().GetName().Return("name").Once()
	// utils.go::getType
	mockColumn2.EXPECT().GetType().Return("string").Once()
	// postgres.go::TypeString
	mockColumn2.EXPECT().GetLength().Return(100).Once()
	// postgres.go::ModifyDefault
	mockColumn2.EXPECT().GetDefault().Return(nil).Once()
	// postgres.go::ModifyIncrement
	mockBlueprint.EXPECT().HasCommand("primary").Return(false).Once()
	mockColumn2.EXPECT().GetType().Return("string").Once()
	// postgres.go::ModifyNullable
	mockColumn2.EXPECT().GetNullable().Return(true).Once()
	mockColumn2.EXPECT().IsChange().Return(false).Times(4)
	mockColumn2.EXPECT().IsSetGeneratedAs().Return(false).Twice()

	s.Equal(`create table "goravel_users" ("id" serial primary key not null, "name" varchar(100) null)`,
		s.grammar.CompileCreate(mockBlueprint))
}

func (s *GrammarSuite) TestCompileDropAllTables() {
	s.Equal([]string{
		`drop table "public"."domain", "public"."users" cascade`,
	}, s.grammar.CompileDropAllTables("public", []contractsdriver.Table{
		{Schema: "public", Name: "domain"},
		{Schema: "public", Name: "users"},
		{Schema: "user", Name: "email"},
	}))
}

func (s *GrammarSuite) TestCompileDropAllTypes() {
	s.Equal([]string{
		`drop type "public"."user" cascade`,
		`drop domain "public"."domain" cascade`,
	}, s.grammar.CompileDropAllTypes("public", []contractsdriver.Type{
		{Schema: "public", Name: "domain", Type: "domain"},
		{Schema: "public", Name: "user"},
		{Schema: "user", Name: "email"},
	}))
}

func (s *GrammarSuite) TestCompileDropAllViews() {
	s.Equal([]string{
		`drop view "public"."domain", "public"."users" cascade`,
	}, s.grammar.CompileDropAllViews("public", []contractsdriver.View{
		{Schema: "public", Name: "domain"},
		{Schema: "public", Name: "users"},
		{Schema: "user", Name: "email"},
	}))
}

func (s *GrammarSuite) TestCompileDropColumn() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockBlueprint.EXPECT().GetTableName().Return("users").Once()

	s.Equal([]string([]string{`alter table "goravel_users" drop column "id", drop column "email"`}), s.grammar.CompileDropColumn(mockBlueprint, &contractsdriver.Command{
		Columns: []string{"id", "email"},
	}))
}

func (s *GrammarSuite) TestCompileDropIfExists() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockBlueprint.EXPECT().GetTableName().Return("users").Once()

	s.Equal(`drop table if exists "goravel_users"`, s.grammar.CompileDropIfExists(mockBlueprint))
}

func (s *GrammarSuite) TestCompileDropPrimary() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockBlueprint.EXPECT().GetTableName().Return("users").Once()

	s.Equal(`alter table "goravel_users" drop constraint "goravel_users_pkey"`, s.grammar.CompileDropPrimary(mockBlueprint, &contractsdriver.Command{}))
}

func (s *GrammarSuite) TestCompileForeign() {
	var mockBlueprint *mocksdriver.Blueprint

	beforeEach := func() {
		mockBlueprint = mocksdriver.NewBlueprint(s.T())
		mockBlueprint.EXPECT().GetTableName().Return("users").Once()
	}

	tests := []struct {
		name      string
		command   *contractsdriver.Command
		expectSql string
	}{
		{
			name: "with on delete and on update",
			command: &contractsdriver.Command{
				Index:      "fk_users_role_id",
				Columns:    []string{"role_id", "user_id"},
				On:         "roles",
				References: []string{"id", "user_id"},
				OnDelete:   "cascade",
				OnUpdate:   "restrict",
			},
			expectSql: `alter table "goravel_users" add constraint "fk_users_role_id" foreign key ("role_id", "user_id") references "goravel_roles" ("id", "user_id") on delete cascade on update restrict`,
		},
		{
			name: "without on delete and on update",
			command: &contractsdriver.Command{
				Index:      "fk_users_role_id",
				Columns:    []string{"role_id", "user_id"},
				On:         "roles",
				References: []string{"id", "user_id"},
			},
			expectSql: `alter table "goravel_users" add constraint "fk_users_role_id" foreign key ("role_id", "user_id") references "goravel_roles" ("id", "user_id")`,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			beforeEach()

			sql := s.grammar.CompileForeign(mockBlueprint, test.command)
			s.Equal(test.expectSql, sql)
		})
	}
}

func (s *GrammarSuite) TestCompileFullText() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockBlueprint.EXPECT().GetTableName().Return("users").Once()

	s.Equal(`create index "users_email_fulltext" on "goravel_users" using gin(to_tsvector('english', "id") || to_tsvector('english', "email"))`, s.grammar.CompileFullText(mockBlueprint, &contractsdriver.Command{
		Index:   "users_email_fulltext",
		Columns: []string{"id", "email"},
	}))
}

func (s *GrammarSuite) TestCompileIndex() {
	var mockBlueprint *mocksdriver.Blueprint

	beforeEach := func() {
		mockBlueprint = mocksdriver.NewBlueprint(s.T())
		mockBlueprint.EXPECT().GetTableName().Return("users").Once()
	}

	tests := []struct {
		name      string
		command   *contractsdriver.Command
		expectSql string
	}{
		{
			name: "with Algorithm",
			command: &contractsdriver.Command{
				Index:     "fk_users_role_id",
				Columns:   []string{"role_id", "user_id"},
				Algorithm: "btree",
			},
			expectSql: `create index "fk_users_role_id" on "goravel_users" using btree ("role_id", "user_id")`,
		},
		{
			name: "without Algorithm",
			command: &contractsdriver.Command{
				Index:   "fk_users_role_id",
				Columns: []string{"role_id", "user_id"},
			},
			expectSql: `create index "fk_users_role_id" on "goravel_users" ("role_id", "user_id")`,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			beforeEach()

			sql := s.grammar.CompileIndex(mockBlueprint, test.command)
			s.Equal(test.expectSql, sql)
		})
	}
}

func (s *GrammarSuite) TestCompileJsonColumnsUpdate() {
	tests := []struct {
		name           string
		values         map[string]any
		expectedValues []map[string]any
		hasError       bool
	}{
		{
			name: "invalid values",
			values: map[string]any{"data->invalid": map[string]any{
				"value": func() {},
			}},
			hasError: true,
		},
		{
			name:   "update single json column",
			values: map[string]any{"data->details": "details value"},
			expectedValues: []map[string]any{
				{"data": databasedb.Raw("jsonb_set(?,?,?)", databasedb.Raw(`"data"::jsonb`), `{"details"}`, `"details value"`)},
			},
		},
		{
			name:   "update single json column(with nested path)",
			values: map[string]any{"data->details->subdetails[0]": "subdetails value"},
			expectedValues: []map[string]any{
				{
					"data": databasedb.Raw(
						"jsonb_set(?,?,?)",
						databasedb.Raw(`"data"::jsonb`),
						`{"details","subdetails",0}`,
						`"subdetails value"`,
					),
				},
			},
		},
		{
			name:   "update multiple json columns",
			values: map[string]any{"data->details": "details value", "data->info": "info value"},
			expectedValues: []map[string]any{
				{
					"data": databasedb.Raw(
						"jsonb_set(?,?,?)",
						databasedb.Raw(
							"jsonb_set(?,?,?)",
							databasedb.Raw(`"data"::jsonb`),
							`{"details"}`, `"details value"`,
						),
						`{"info"}`, `"info value"`,
					),
				},
				{
					"data": databasedb.Raw(
						"jsonb_set(?,?,?)",
						databasedb.Raw(
							"jsonb_set(?,?,?)",
							databasedb.Raw(`"data"::jsonb`),
							`{"info"}`, `"info value"`,
						),
						`{"details"}`, `"details value"`,
					),
				},
			},
		},
	}

	mockApp := mocksfoundation.NewApplication(s.T())

	originApp := App
	App = mockApp
	s.T().Cleanup(func() {
		App = originApp
	})

	for _, tt := range tests {
		s.Run(tt.name, func() {
			mockApp.EXPECT().GetJson().Return(json.New()).Once()
			actualValues, err := s.grammar.CompileJsonColumnsUpdate(tt.values)
			if tt.hasError {
				s.Error(err)
			} else {
				s.Subset(tt.expectedValues, []any{actualValues})
				s.NoError(err)
			}
		})
	}
}

func (s *GrammarSuite) TestCompileJsonContains() {
	tests := []struct {
		name          string
		column        string
		value         any
		isNot         bool
		expectedSql   string
		expectedValue []any
		hasError      bool
	}{
		{
			name:     "invalid value type",
			column:   "data->details",
			value:    func() {},
			hasError: true,
		},
		{
			name:          "single path with single value",
			column:        "data->details",
			value:         "value1",
			expectedSql:   `("data"->'details')::jsonb @> ?`,
			expectedValue: []any{`"value1"`},
		},
		{
			name:          "single path with multiple values",
			column:        "data->details",
			value:         []string{"value1", "value2"},
			expectedSql:   `("data"->'details')::jsonb @> ?`,
			expectedValue: []any{`["value1","value2"]`},
		},
		{
			name:          "nested path with single value",
			column:        "data->details->subdetails[0]",
			value:         "value1",
			expectedSql:   `("data"->'details'->'subdetails'->0)::jsonb @> ?`,
			expectedValue: []any{`"value1"`},
		},
		{
			name:          "nested path with multiple values",
			column:        "data->details[0]->subdetails",
			value:         []string{"value1", "value2"},
			expectedSql:   `("data"->'details'->0->'subdetails')::jsonb @> ?`,
			expectedValue: []any{`["value1","value2"]`},
		},
		{
			name:          "with is not condition",
			column:        "data->details",
			value:         "value1",
			isNot:         true,
			expectedSql:   `not ("data"->'details')::jsonb @> ?`,
			expectedValue: []any{`"value1"`},
		},
	}

	mockApp := mocksfoundation.NewApplication(s.T())
	originApp := App
	App = mockApp
	s.T().Cleanup(func() {
		App = originApp
	})

	for _, tt := range tests {
		s.Run(tt.name, func() {
			mockApp.EXPECT().GetJson().Return(json.New()).Once()
			actualSql, actualValue, err := s.grammar.CompileJsonContains(tt.column, tt.value, tt.isNot)
			if tt.hasError {
				s.Error(err)
			} else {
				s.Equal(tt.expectedSql, actualSql)
				s.Equal(tt.expectedValue, actualValue)
				s.NoError(err)
			}
		})
	}
}

func (s *GrammarSuite) TestCompileJsonContainKey() {
	tests := []struct {
		name        string
		column      string
		isNot       bool
		expectedSql string
	}{
		{
			name:        "single path",
			column:      "data->details",
			expectedSql: `coalesce(("data")::jsonb ? 'details', false)`,
		},
		{
			name:        "single path with is not",
			column:      "data->details",
			isNot:       true,
			expectedSql: `not coalesce(("data")::jsonb ? 'details', false)`,
		},
		{
			name:        "nested path",
			column:      "data->details->subdetails",
			expectedSql: `coalesce(("data"->'details')::jsonb ? 'subdetails', false)`,
		},
		{
			name:        "nested path with array index",
			column:      "data->details[0]->subdetails",
			expectedSql: `coalesce(("data"->'details'->0)::jsonb ? 'subdetails', false)`,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.expectedSql, s.grammar.CompileJsonContainsKey(tt.column, tt.isNot))
		})
	}
}

func (s *GrammarSuite) TestCompileJsonLength() {
	tests := []struct {
		name        string
		column      string
		expectedSql string
	}{
		{
			name:        "single path",
			column:      "data->details",
			expectedSql: `jsonb_array_length(("data"->'details')::jsonb)`,
		},
		{
			name:        "nested path",
			column:      "data->details->subdetails",
			expectedSql: `jsonb_array_length(("data"->'details'->'subdetails')::jsonb)`,
		},
		{
			name:        "nested path with array index",
			column:      "data->details[0]->subdetails",
			expectedSql: `jsonb_array_length(("data"->'details'->0->'subdetails')::jsonb)`,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.expectedSql, s.grammar.CompileJsonLength(tt.column))
		})
	}
}

func (s *GrammarSuite) TestCompileJsonValues() {
	tests := []struct {
		name     string
		args     []any
		expected []any
	}{
		{
			name:     "number values",
			args:     []any{1},
			expected: []any{"1"},
		},
		{
			name:     "number values",
			args:     []any{[]int{1, 2, 3}},
			expected: []any{[]any{"1", "2", "3"}},
		},
		{
			name:     "string values",
			args:     []any{"value1", "value2", "value3"},
			expected: []any{"value1", "value2", "value3"},
		},
		{
			name:     "boolean values",
			args:     []any{true, false},
			expected: []any{"true", "false"},
		},
		{
			name:     "pointer values",
			args:     []any{convert.Pointer(123)},
			expected: []any{"123"},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.expected, s.grammar.CompileJsonValues(tt.args...))
		})
	}
}

func (s *GrammarSuite) TestCompilePrimary() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockBlueprint.EXPECT().GetTableName().Return("users").Once()

	s.Equal(`alter table "goravel_users" add primary key ("role_id", "user_id")`, s.grammar.CompilePrimary(mockBlueprint, &contractsdriver.Command{
		Columns: []string{"role_id", "user_id"},
	}))
}

func (s *GrammarSuite) TestCompileRenameColumn() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockColumn := mocksdriver.NewColumnDefinition(s.T())

	mockBlueprint.EXPECT().GetTableName().Return("users").Once()

	sql, err := s.grammar.CompileRenameColumn(mockBlueprint, &contractsdriver.Command{
		Column: mockColumn,
		From:   "before",
		To:     "after",
	}, nil)

	s.NoError(err)
	s.Equal(`alter table "goravel_users" rename column "before" to "after"`, sql)
}

func (s *GrammarSuite) TestCompileUnique() {
	tests := []struct {
		name               string
		deferrable         *bool
		initiallyImmediate *bool
		expectSql          string
	}{
		{
			name:               "with deferrable and initially immediate",
			deferrable:         convert.Pointer(true),
			initiallyImmediate: convert.Pointer(true),
			expectSql:          `alter table "goravel_users" add constraint "unique_users_email" unique ("id", "email") deferrable initially immediate`,
		},
		{
			name:               "with deferrable and initially immediate, both false",
			deferrable:         convert.Pointer(false),
			initiallyImmediate: convert.Pointer(false),
			expectSql:          `alter table "goravel_users" add constraint "unique_users_email" unique ("id", "email") not deferrable initially deferred`,
		},
		{
			name:      "without deferrable and initially immediate",
			expectSql: `alter table "goravel_users" add constraint "unique_users_email" unique ("id", "email")`,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			mockBlueprint := mocksdriver.NewBlueprint(s.T())
			mockBlueprint.EXPECT().GetTableName().Return("users").Once()

			sql := s.grammar.CompileUnique(mockBlueprint, &contractsdriver.Command{
				Index:              "unique_users_email",
				Columns:            []string{"id", "email"},
				Deferrable:         test.deferrable,
				InitiallyImmediate: test.initiallyImmediate,
			})

			s.Equal(test.expectSql, sql)
		})
	}
}

func (s *GrammarSuite) TestGetColumns() {
	mockColumn1 := mocksdriver.NewColumnDefinition(s.T())
	mockColumn2 := mocksdriver.NewColumnDefinition(s.T())
	mockBlueprint := mocksdriver.NewBlueprint(s.T())

	mockBlueprint.EXPECT().GetAddedColumns().Return([]contractsdriver.ColumnDefinition{
		mockColumn1, mockColumn2,
	}).Once()
	mockBlueprint.EXPECT().HasCommand("primary").Return(false).Twice()

	mockColumn1.EXPECT().GetName().Return("id").Once()
	mockColumn1.EXPECT().GetType().Return("integer").Twice()
	mockColumn1.EXPECT().GetDefault().Return(nil).Once()
	mockColumn1.EXPECT().GetNullable().Return(false).Once()
	mockColumn1.EXPECT().GetAutoIncrement().Return(true).Twice()
	mockColumn1.EXPECT().IsChange().Return(false).Times(5)
	mockColumn1.EXPECT().IsSetGeneratedAs().Return(false).Twice()

	mockColumn2.EXPECT().GetName().Return("name").Once()
	mockColumn2.EXPECT().GetType().Return("string").Twice()
	mockColumn2.EXPECT().GetDefault().Return("goravel").Twice()
	mockColumn2.EXPECT().GetNullable().Return(true).Once()
	mockColumn2.EXPECT().GetLength().Return(10).Once()
	mockColumn2.EXPECT().IsChange().Return(false).Times(4)
	mockColumn2.EXPECT().IsSetGeneratedAs().Return(false).Twice()

	s.Equal([]string{"\"id\" serial primary key not null", "\"name\" varchar(10) default 'goravel' null"}, s.grammar.getColumns(mockBlueprint))
}

func (s *GrammarSuite) TestEscapeNames() {
	// SingleName
	names := []string{"username"}
	expected := []string{`"username"`}
	s.Equal(expected, s.grammar.EscapeNames(names))

	// MultipleNames
	names = []string{"username", "user.email"}
	expected = []string{`"username"`, `"user"."email"`}
	s.Equal(expected, s.grammar.EscapeNames(names))

	// NamesEmpty
	names = []string{}
	expected = []string{}
	s.Equal(expected, s.grammar.EscapeNames(names))
}

func (s *GrammarSuite) TestModifyDefault() {
	var (
		mockBlueprint *mocksdriver.Blueprint
		mockColumn    *mocksdriver.ColumnDefinition
	)

	tests := []struct {
		name      string
		setup     func()
		expectSql string
	}{
		{
			name: "without change and default is nil",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(false).Once()
				mockColumn.EXPECT().GetDefault().Return(nil).Once()
			},
		},
		{
			name: "with change and auto increment",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(true).Once()
				mockColumn.EXPECT().GetAutoIncrement().Return(true).Once()
			},
		},
		{
			name: "with change and default is nil",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(true).Once()
				mockColumn.EXPECT().GetAutoIncrement().Return(false).Once()
				mockColumn.EXPECT().GetDefault().Return(nil).Once()
				mockColumn.EXPECT().IsSetGeneratedAs().Return(false).Once()
			},
			expectSql: " drop default",
		},
		{
			name: "without change and default is not nil",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(false).Once()
				mockColumn.EXPECT().GetDefault().Return("goravel").Twice()
			},
			expectSql: " default 'goravel'",
		},
		{
			name: "with change and default is not nil",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(true).Once()
				mockColumn.EXPECT().GetAutoIncrement().Return(false).Once()
				mockColumn.EXPECT().GetDefault().Return("goravel").Twice()
				mockColumn.EXPECT().IsSetGeneratedAs().Return(false).Once()
			},
			expectSql: " set default 'goravel'",
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			mockBlueprint = mocksdriver.NewBlueprint(s.T())
			mockColumn = mocksdriver.NewColumnDefinition(s.T())

			test.setup()

			sql := s.grammar.ModifyDefault(mockBlueprint, mockColumn)

			s.Equal(test.expectSql, sql)
		})
	}
}

func (s *GrammarSuite) TestModifyGeneratedAs() {
	var (
		mockBlueprint *mocksdriver.Blueprint
		mockColumn    *mocksdriver.ColumnDefinition
	)

	tests := []struct {
		name      string
		setup     func()
		expectSql []string
	}{
		{
			name: "generated by default",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(false).Twice()
				mockColumn.EXPECT().IsSetGeneratedAs().Return(true).Once()
				mockColumn.EXPECT().IsAlways().Return(false).Once()
				mockColumn.EXPECT().GetGeneratedAs().Return("").Once()
			},
			expectSql: []string{" generated by default as identity"},
		},
		{
			name: "generated always",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(false).Twice()
				mockColumn.EXPECT().IsSetGeneratedAs().Return(true).Once()
				mockColumn.EXPECT().IsAlways().Return(true).Once()
				mockColumn.EXPECT().GetGeneratedAs().Return("").Once()
			},
			expectSql: []string{" generated always as identity"},
		},
		{
			name: "generated by default with expression",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(false).Twice()
				mockColumn.EXPECT().IsSetGeneratedAs().Return(true).Once()
				mockColumn.EXPECT().IsAlways().Return(false).Once()
				mockColumn.EXPECT().GetGeneratedAs().Return("START WITH 1000 INCREMENT BY 5").Once()
			},
			expectSql: []string{" generated by default as identity (START WITH 1000 INCREMENT BY 5)"},
		},
		{
			name: "generated always with expression",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(false).Twice()
				mockColumn.EXPECT().IsSetGeneratedAs().Return(true).Once()
				mockColumn.EXPECT().IsAlways().Return(true).Once()
				mockColumn.EXPECT().GetGeneratedAs().Return("START WITH 1000 INCREMENT BY 5").Once()
			},
			expectSql: []string{" generated always as identity (START WITH 1000 INCREMENT BY 5)"},
		},
		{
			name: "generated always with expression for change",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(true).Twice()
				mockColumn.EXPECT().GetAutoIncrement().Return(false).Once()
				mockColumn.EXPECT().IsSetGeneratedAs().Return(true).Twice()
				mockColumn.EXPECT().IsAlways().Return(true).Once()
				mockColumn.EXPECT().GetGeneratedAs().Return("START WITH 1000 INCREMENT BY 5").Once()
			},
			expectSql: []string{" drop identity if exists", " add generated always as identity (START WITH 1000 INCREMENT BY 5)"},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			mockBlueprint = mocksdriver.NewBlueprint(s.T())
			mockColumn = mocksdriver.NewColumnDefinition(s.T())

			test.setup()

			var actualSql []string
			if sql := s.grammar.ModifyGeneratedAsForChange(mockBlueprint, mockColumn); len(sql) > 0 {
				actualSql = append(actualSql, sql)
			}
			if sql := s.grammar.ModifyGeneratedAs(mockBlueprint, mockColumn); len(sql) > 0 {
				actualSql = append(actualSql, sql)
			}

			s.Equal(test.expectSql, actualSql)
		})
	}
}

func (s *GrammarSuite) TestModifyNullable() {
	var (
		mockBlueprint *mocksdriver.Blueprint
		mockColumn    *mocksdriver.ColumnDefinition
	)

	tests := []struct {
		name      string
		setup     func()
		expectSql string
	}{
		{
			name: "without change and nullable",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(false).Once()
				mockColumn.EXPECT().GetNullable().Return(true).Once()
			},
			expectSql: " null",
		},
		{
			name: "with change and and nullable",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(true).Once()
				mockColumn.EXPECT().GetNullable().Return(true).Once()
			},
			expectSql: " drop not null",
		},
		{
			name: "without change and not nullable",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(false).Once()
				mockColumn.EXPECT().GetNullable().Return(false).Once()
			},
			expectSql: " not null",
		},
		{
			name: "with change and not nullable",
			setup: func() {
				mockColumn.EXPECT().IsChange().Return(true).Once()
				mockColumn.EXPECT().GetNullable().Return(false).Once()
			},
			expectSql: " set not null",
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			mockBlueprint = mocksdriver.NewBlueprint(s.T())
			mockColumn = mocksdriver.NewColumnDefinition(s.T())

			test.setup()

			sql := s.grammar.ModifyNullable(mockBlueprint, mockColumn)

			s.Equal(test.expectSql, sql)
		})
	}
}

func (s *GrammarSuite) TestModifyIncrement() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())

	mockColumn := mocksdriver.NewColumnDefinition(s.T())
	mockBlueprint.EXPECT().HasCommand("primary").Return(false).Once()
	mockColumn.EXPECT().GetType().Return("bigInteger").Once()
	mockColumn.EXPECT().GetAutoIncrement().Return(true).Once()
	mockColumn.EXPECT().IsChange().Return(false).Once()

	s.Equal(" primary key", s.grammar.ModifyIncrement(mockBlueprint, mockColumn))
}

func (s *GrammarSuite) TestTableComment() {
	mockBlueprint := mocksdriver.NewBlueprint(s.T())
	mockBlueprint.EXPECT().GetTableName().Return("users").Once()

	s.Equal(`comment on table "goravel_users" is 'It''s a table comment'`, s.grammar.CompileTableComment(mockBlueprint, &contractsdriver.Command{
		Value: "It's a table comment",
	}))
}

func (s *GrammarSuite) TestTypeBigInteger() {
	mockColumn1 := mocksdriver.NewColumnDefinition(s.T())
	mockColumn1.EXPECT().GetAutoIncrement().Return(true).Once()
	mockColumn1.EXPECT().IsChange().Return(false).Once()
	mockColumn1.EXPECT().IsSetGeneratedAs().Return(false).Once()

	s.Equal("bigserial", s.grammar.TypeBigInteger(mockColumn1))

	mockColumn2 := mocksdriver.NewColumnDefinition(s.T())
	mockColumn2.EXPECT().GetAutoIncrement().Return(false).Once()

	s.Equal("bigint", s.grammar.TypeBigInteger(mockColumn2))
}

func (s *GrammarSuite) TestTypeDecimal() {
	mockColumn := mocksdriver.NewColumnDefinition(s.T())
	mockColumn.EXPECT().GetTotal().Return(4).Once()
	mockColumn.EXPECT().GetPlaces().Return(2).Once()

	s.Equal("decimal(4, 2)", s.grammar.TypeDecimal(mockColumn))
}

func (s *GrammarSuite) TestTypeEnum() {
	mockColumn := mocksdriver.NewColumnDefinition(s.T())
	mockColumn.EXPECT().GetName().Return("a").Once()
	mockColumn.EXPECT().GetAllowed().Return([]any{"a", "b"}).Once()

	s.Equal(`varchar(255) check ("a" in ('a', 'b'))`, s.grammar.TypeEnum(mockColumn))
}

func (s *GrammarSuite) TestTypeFloat() {
	mockColumn := mocksdriver.NewColumnDefinition(s.T())
	mockColumn.EXPECT().GetPrecision().Return(0).Once()

	s.Equal("float", s.grammar.TypeFloat(mockColumn))

	mockColumn.EXPECT().GetPrecision().Return(2).Once()

	s.Equal("float(2)", s.grammar.TypeFloat(mockColumn))
}

func (s *GrammarSuite) TestTypeInteger() {
	mockColumn1 := mocksdriver.NewColumnDefinition(s.T())
	mockColumn1.EXPECT().GetAutoIncrement().Return(true).Once()
	mockColumn1.EXPECT().IsChange().Return(false).Once()
	mockColumn1.EXPECT().IsSetGeneratedAs().Return(false).Once()

	s.Equal("serial", s.grammar.TypeInteger(mockColumn1))

	mockColumn2 := mocksdriver.NewColumnDefinition(s.T())
	mockColumn2.EXPECT().GetAutoIncrement().Return(false).Once()

	s.Equal("integer", s.grammar.TypeInteger(mockColumn2))
}

func (s *GrammarSuite) TestTypeString() {
	mockColumn1 := mocksdriver.NewColumnDefinition(s.T())
	mockColumn1.EXPECT().GetLength().Return(100).Once()

	s.Equal("varchar(100)", s.grammar.TypeString(mockColumn1))

	mockColumn2 := mocksdriver.NewColumnDefinition(s.T())
	mockColumn2.EXPECT().GetLength().Return(0).Once()

	s.Equal("varchar", s.grammar.TypeString(mockColumn2))
}

func (s *GrammarSuite) TestTypeTimestamp() {
	mockColumn := mocksdriver.NewColumnDefinition(s.T())
	mockColumn.EXPECT().GetUseCurrent().Return(true).Once()
	mockColumn.EXPECT().Default(schema.Expression("CURRENT_TIMESTAMP")).Return(mockColumn).Once()
	mockColumn.EXPECT().GetPrecision().Return(3).Once()
	s.Equal("timestamp(3) without time zone", s.grammar.TypeTimestamp(mockColumn))
}

func (s *GrammarSuite) TestTypeTimestampTz() {
	mockColumn := mocksdriver.NewColumnDefinition(s.T())
	mockColumn.EXPECT().GetUseCurrent().Return(true).Once()
	mockColumn.EXPECT().Default(schema.Expression("CURRENT_TIMESTAMP")).Return(mockColumn).Once()
	mockColumn.EXPECT().GetPrecision().Return(3).Once()
	s.Equal("timestamp(3) with time zone", s.grammar.TypeTimestampTz(mockColumn))
}

func (s *GrammarSuite) TestTypeUuid() {
	mockColumn := mocksdriver.NewColumnDefinition(s.T())
	s.Equal("uuid", s.grammar.TypeUuid(mockColumn))
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
