package postgres

import (
	"fmt"
	"slices"

	"github.com/goravel/framework/contracts/database/orm"
	contractsschema "github.com/goravel/framework/contracts/database/schema"
)

type Schema struct {
	grammar   *Grammar
	orm       orm.Orm
	prefix    string
	processor *Processor
	schema    string
}

func NewSchema(grammar *Grammar, orm orm.Orm, schema, prefix string) *Schema {
	return &Schema{
		grammar:   grammar,
		orm:       orm,
		prefix:    prefix,
		processor: NewProcessor(),
		schema:    schema,
	}
}

func (r *Schema) DropAllTables() error {
	excludedTables := r.grammar.EscapeNames([]string{"spatial_ref_sys"})
	schema := r.grammar.EscapeNames([]string{r.schema})[0]

	tables, err := r.GetTables()
	if err != nil {
		return err
	}

	var dropTables []string
	for _, table := range tables {
		qualifiedName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

		isExcludedTable := slices.Contains(excludedTables, qualifiedName) || slices.Contains(excludedTables, table.Name)
		isInCurrentSchema := schema == r.grammar.EscapeNames([]string{table.Schema})[0]

		if !isExcludedTable && isInCurrentSchema {
			dropTables = append(dropTables, qualifiedName)
		}
	}

	if len(dropTables) == 0 {
		return nil
	}

	_, err = r.orm.Query().Exec(r.grammar.CompileDropAllTables(dropTables))

	return err
}

func (r *Schema) DropAllTypes() error {
	types, err := r.GetTypes()
	if err != nil {
		return err
	}

	var dropTypes, dropDomains []string

	for _, t := range types {
		if !t.Implicit && r.schema == t.Schema {
			if t.Type == "domain" {
				dropDomains = append(dropDomains, fmt.Sprintf("%s.%s", t.Schema, t.Name))
			} else {
				dropTypes = append(dropTypes, fmt.Sprintf("%s.%s", t.Schema, t.Name))
			}
		}
	}

	return r.orm.Transaction(func(tx orm.Query) error {
		if len(dropTypes) > 0 {
			if _, err := tx.Exec(r.grammar.CompileDropAllTypes(dropTypes)); err != nil {
				return err
			}
		}

		if len(dropDomains) > 0 {
			if _, err := tx.Exec(r.grammar.CompileDropAllDomains(dropDomains)); err != nil {
				return err
			}
		}

		return nil
	})
}

func (r *Schema) DropAllViews() error {
	views, err := r.GetViews()
	if err != nil {
		return err
	}

	var dropViews []string
	for _, view := range views {
		if r.schema == view.Schema {
			dropViews = append(dropViews, fmt.Sprintf("%s.%s", view.Schema, view.Name))
		}
	}
	if len(dropViews) == 0 {
		return nil
	}

	_, err = r.orm.Query().Exec(r.grammar.CompileDropAllViews(dropViews))

	return err
}

func (r *Schema) GetColumns(table string) ([]contractsschema.Column, error) {
	schema, table, err := parseSchemaAndTable(table, r.schema)
	if err != nil {
		return nil, err
	}

	table = r.prefix + table

	var dbColumns []contractsschema.DBColumn
	if err := r.orm.Query().Raw(r.grammar.CompileColumns(schema, table)).Scan(&dbColumns); err != nil {
		return nil, err
	}

	return r.processor.ProcessColumns(dbColumns), nil
}

func (r *Schema) GetIndexes(table string) ([]contractsschema.Index, error) {
	schema, table, err := parseSchemaAndTable(table, r.schema)
	if err != nil {
		return nil, err
	}

	table = r.prefix + table

	var dbIndexes []contractsschema.DBIndex
	if err := r.orm.Query().Raw(r.grammar.CompileIndexes(schema, table)).Scan(&dbIndexes); err != nil {
		return nil, err
	}

	return r.processor.ProcessIndexes(dbIndexes), nil
}

func (r *Schema) GetTables() ([]contractsschema.Table, error) {
	var tables []contractsschema.Table
	if err := r.orm.Query().Raw(r.grammar.CompileTables(r.orm.DatabaseName())).Scan(&tables); err != nil {
		return nil, err
	}

	return tables, nil
}

func (r *Schema) GetTypes() ([]contractsschema.Type, error) {
	var types []contractsschema.Type
	if err := r.orm.Query().Raw(r.grammar.CompileTypes()).Scan(&types); err != nil {
		return nil, err
	}

	return r.processor.ProcessTypes(types), nil
}

func (r *Schema) GetViews() ([]contractsschema.View, error) {
	var views []contractsschema.View
	if err := r.orm.Query().Raw(r.grammar.CompileViews(r.orm.DatabaseName())).Scan(&views); err != nil {
		return nil, err
	}

	return views, nil
}
