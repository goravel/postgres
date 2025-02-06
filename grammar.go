package postgres

import (
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/cast"

	contractsschema "github.com/goravel/framework/contracts/database/schema"
	"github.com/goravel/framework/database/schema"
	"github.com/goravel/framework/errors"
	"github.com/goravel/framework/support/collect"
)

var _ contractsschema.Grammar = &Grammar{}

type Grammar struct {
	attributeCommands []string
	modifiers         []func(contractsschema.Blueprint, contractsschema.ColumnDefinition) string
	prefix            string
	serials           []string
	wrap              *schema.Wrap
}

func NewGrammar(prefix string) *Grammar {
	grammar := &Grammar{
		attributeCommands: []string{schema.CommandComment},
		prefix:            prefix,
		serials:           []string{"bigInteger", "integer", "mediumInteger", "smallInteger", "tinyInteger"},
		wrap:              schema.NewWrap(prefix),
	}
	grammar.modifiers = []func(contractsschema.Blueprint, contractsschema.ColumnDefinition) string{
		grammar.ModifyDefault,
		grammar.ModifyIncrement,
		grammar.ModifyNullable,
	}

	return grammar
}

func (r *Grammar) CompileAdd(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s add column %s", r.wrap.Table(blueprint.GetTableName()), r.getColumn(blueprint, command.Column))
}

func (r *Grammar) CompileChange(blueprint contractsschema.Blueprint, command *contractsschema.Command) []string {
	changes := []string{fmt.Sprintf("alter column %s type %s", r.wrap.Column(command.Column.GetName()), schema.ColumnType(r, command.Column))}
	for _, modifier := range r.modifiers {
		if change := modifier(blueprint, command.Column); change != "" {
			changes = append(changes, fmt.Sprintf("alter column %s%s", r.wrap.Column(command.Column.GetName()), change))
		}
	}

	return []string{
		fmt.Sprintf("alter table %s %s", r.wrap.Table(blueprint.GetTableName()), strings.Join(changes, ", ")),
	}
}

func (r *Grammar) CompileColumns(schema, table string) (string, error) {
	schema, table, err := parseSchemaAndTable(table, schema)
	if err != nil {
		return "", err
	}

	table = r.prefix + table

	return fmt.Sprintf(
		"select a.attname as name, t.typname as type_name, format_type(a.atttypid, a.atttypmod) as type, "+
			"(select tc.collcollate from pg_catalog.pg_collation tc where tc.oid = a.attcollation) as collation, "+
			"not a.attnotnull as nullable, "+
			"(select pg_get_expr(adbin, adrelid) from pg_attrdef where c.oid = pg_attrdef.adrelid and pg_attrdef.adnum = a.attnum) as default, "+
			"col_description(c.oid, a.attnum) as comment "+
			"from pg_attribute a, pg_class c, pg_type t, pg_namespace n "+
			"where c.relname = %s and n.nspname = %s and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid and n.oid = c.relnamespace "+
			"order by a.attnum", r.wrap.Quote(table), r.wrap.Quote(schema)), nil
}

func (r *Grammar) CompileComment(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	comment := "NULL"
	if command.Column.IsSetComment() {
		comment = r.wrap.Quote(strings.ReplaceAll(command.Column.GetComment(), "'", "''"))
	}

	return fmt.Sprintf("comment on column %s.%s is %s",
		r.wrap.Table(blueprint.GetTableName()),
		r.wrap.Column(command.Column.GetName()),
		comment)
}

func (r *Grammar) CompileCreate(blueprint contractsschema.Blueprint) string {
	return fmt.Sprintf("create table %s (%s)", r.wrap.Table(blueprint.GetTableName()), strings.Join(r.getColumns(blueprint), ", "))
}

func (r *Grammar) CompileDefault(_ contractsschema.Blueprint, _ *contractsschema.Command) string {
	return ""
}

func (r *Grammar) CompileDrop(blueprint contractsschema.Blueprint) string {
	return fmt.Sprintf("drop table %s", r.wrap.Table(blueprint.GetTableName()))
}

func (r *Grammar) CompileDropAllDomains(domains []string) string {
	return fmt.Sprintf("drop domain %s cascade", strings.Join(r.EscapeNames(domains), ", "))
}

func (r *Grammar) CompileDropAllTables(schema string, tables []contractsschema.Table) []string {
	excludedTables := r.EscapeNames([]string{"spatial_ref_sys"})
	escapedSchema := r.EscapeNames([]string{schema})[0]

	var dropTables []string
	for _, table := range tables {
		qualifiedName := fmt.Sprintf("%s.%s", table.Schema, table.Name)

		isExcludedTable := slices.Contains(excludedTables, qualifiedName) || slices.Contains(excludedTables, table.Name)
		isInCurrentSchema := escapedSchema == r.EscapeNames([]string{table.Schema})[0]

		if !isExcludedTable && isInCurrentSchema {
			dropTables = append(dropTables, qualifiedName)
		}
	}

	if len(dropTables) == 0 {
		return nil
	}

	return []string{fmt.Sprintf("drop table %s cascade", strings.Join(r.EscapeNames(dropTables), ", "))}
}

func (r *Grammar) CompileDropAllTypes(schema string, types []contractsschema.Type) []string {
	var dropTypes, dropDomains []string

	for _, t := range types {
		if !t.Implicit && schema == t.Schema {
			if t.Type == "domain" {
				dropDomains = append(dropDomains, fmt.Sprintf("%s.%s", t.Schema, t.Name))
			} else {
				dropTypes = append(dropTypes, fmt.Sprintf("%s.%s", t.Schema, t.Name))
			}
		}
	}

	var sql []string
	if len(dropTypes) > 0 {
		sql = append(sql, fmt.Sprintf("drop type %s cascade", strings.Join(r.EscapeNames(dropTypes), ", ")))
	}
	if len(dropDomains) > 0 {
		sql = append(sql, fmt.Sprintf("drop domain %s cascade", strings.Join(r.EscapeNames(dropDomains), ", ")))
	}

	return sql
}

func (r *Grammar) CompileDropAllViews(schema string, views []contractsschema.View) []string {
	var dropViews []string
	for _, view := range views {
		if schema == view.Schema {
			dropViews = append(dropViews, fmt.Sprintf("%s.%s", view.Schema, view.Name))
		}
	}
	if len(dropViews) == 0 {
		return nil
	}

	return []string{fmt.Sprintf("drop view %s cascade", strings.Join(r.EscapeNames(dropViews), ", "))}
}

func (r *Grammar) CompileDropColumn(blueprint contractsschema.Blueprint, command *contractsschema.Command) []string {
	columns := r.wrap.PrefixArray("drop column", r.wrap.Columns(command.Columns))

	return []string{
		fmt.Sprintf("alter table %s %s", r.wrap.Table(blueprint.GetTableName()), strings.Join(columns, ", ")),
	}
}

func (r *Grammar) CompileDropForeign(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s drop constraint %s", r.wrap.Table(blueprint.GetTableName()), r.wrap.Column(command.Index))
}

func (r *Grammar) CompileDropFullText(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return r.CompileDropIndex(blueprint, command)
}

func (r *Grammar) CompileDropIfExists(blueprint contractsschema.Blueprint) string {
	return fmt.Sprintf("drop table if exists %s", r.wrap.Table(blueprint.GetTableName()))
}

func (r *Grammar) CompileDropIndex(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("drop index %s", r.wrap.Column(command.Index))
}

func (r *Grammar) CompileDropPrimary(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	tableName := blueprint.GetTableName()
	index := r.wrap.Column(fmt.Sprintf("%s%s_pkey", r.wrap.GetPrefix(), tableName))

	return fmt.Sprintf("alter table %s drop constraint %s", r.wrap.Table(tableName), index)
}

func (r *Grammar) CompileDropUnique(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s drop constraint %s", r.wrap.Table(blueprint.GetTableName()), r.wrap.Column(command.Index))
}

func (r *Grammar) CompileForeign(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	sql := fmt.Sprintf("alter table %s add constraint %s foreign key (%s) references %s (%s)",
		r.wrap.Table(blueprint.GetTableName()),
		r.wrap.Column(command.Index),
		r.wrap.Columnize(command.Columns),
		r.wrap.Table(command.On),
		r.wrap.Columnize(command.References))
	if command.OnDelete != "" {
		sql += " on delete " + command.OnDelete
	}
	if command.OnUpdate != "" {
		sql += " on update " + command.OnUpdate
	}

	return sql
}

func (r *Grammar) CompileForeignKeys(schema, table string) string {
	return fmt.Sprintf(
		`SELECT 
			c.conname AS name, 
			string_agg(la.attname, ',' ORDER BY conseq.ord) AS columns, 
			fn.nspname AS foreign_schema, 
			fc.relname AS foreign_table, 
			string_agg(fa.attname, ',' ORDER BY conseq.ord) AS foreign_columns, 
			c.confupdtype AS on_update, 
			c.confdeltype AS on_delete 
		FROM pg_constraint c 
		JOIN pg_class tc ON c.conrelid = tc.oid 
		JOIN pg_namespace tn ON tn.oid = tc.relnamespace 
		JOIN pg_class fc ON c.confrelid = fc.oid 
		JOIN pg_namespace fn ON fn.oid = fc.relnamespace 
		JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS conseq(num, ord) ON TRUE 
		JOIN pg_attribute la ON la.attrelid = c.conrelid AND la.attnum = conseq.num 
		JOIN pg_attribute fa ON fa.attrelid = c.confrelid AND fa.attnum = c.confkey[conseq.ord] 
		WHERE c.contype = 'f' AND tc.relname = %s AND tn.nspname = %s 
		GROUP BY c.conname, fn.nspname, fc.relname, c.confupdtype, c.confdeltype`,
		r.wrap.Quote(table),
		r.wrap.Quote(schema),
	)
}

func (r *Grammar) CompileFullText(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	language := "english"
	if command.Language != "" {
		language = command.Language
	}

	columns := collect.Map(command.Columns, func(column string, _ int) string {
		return fmt.Sprintf("to_tsvector(%s, %s)", r.wrap.Quote(language), r.wrap.Column(column))
	})

	return fmt.Sprintf("create index %s on %s using gin(%s)", r.wrap.Column(command.Index), r.wrap.Table(blueprint.GetTableName()), strings.Join(columns, " || "))
}

func (r *Grammar) CompileIndex(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	var algorithm string
	if command.Algorithm != "" {
		algorithm = " using " + command.Algorithm
	}

	return fmt.Sprintf("create index %s on %s%s (%s)",
		r.wrap.Column(command.Index),
		r.wrap.Table(blueprint.GetTableName()),
		algorithm,
		r.wrap.Columnize(command.Columns),
	)
}

func (r *Grammar) CompileIndexes(schema, table string) (string, error) {
	schema, table, err := parseSchemaAndTable(table, schema)
	if err != nil {
		return "", err
	}

	table = r.prefix + table

	return fmt.Sprintf(
		"select ic.relname as name, string_agg(a.attname, ',' order by indseq.ord) as columns, "+
			"am.amname as \"type\", i.indisunique as \"unique\", i.indisprimary as \"primary\" "+
			"from pg_index i "+
			"join pg_class tc on tc.oid = i.indrelid "+
			"join pg_namespace tn on tn.oid = tc.relnamespace "+
			"join pg_class ic on ic.oid = i.indexrelid "+
			"join pg_am am on am.oid = ic.relam "+
			"join lateral unnest(i.indkey) with ordinality as indseq(num, ord) on true "+
			"left join pg_attribute a on a.attrelid = i.indrelid and a.attnum = indseq.num "+
			"where tc.relname = %s and tn.nspname = %s "+
			"group by ic.relname, am.amname, i.indisunique, i.indisprimary",
		r.wrap.Quote(table),
		r.wrap.Quote(schema),
	), nil
}

func (r *Grammar) CompilePrimary(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s add primary key (%s)", r.wrap.Table(blueprint.GetTableName()), r.wrap.Columnize(command.Columns))
}

func (r *Grammar) CompileRename(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s rename to %s", r.wrap.Table(blueprint.GetTableName()), r.wrap.Table(command.To))
}

func (r *Grammar) CompileRenameColumn(_ contractsschema.Schema, blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	return fmt.Sprintf("alter table %s rename column %s to %s",
		r.wrap.Table(blueprint.GetTableName()),
		r.wrap.Column(command.From),
		r.wrap.Column(command.To),
	)
}

func (r *Grammar) CompileRenameIndex(_ contractsschema.Schema, _ contractsschema.Blueprint, command *contractsschema.Command) []string {
	return []string{
		fmt.Sprintf("alter index %s rename to %s", r.wrap.Column(command.From), r.wrap.Column(command.To)),
	}
}

func (r *Grammar) CompileTables(_ string) string {
	return "select c.relname as name, n.nspname as schema, pg_total_relation_size(c.oid) as size, " +
		"obj_description(c.oid, 'pg_class') as comment from pg_class c, pg_namespace n " +
		"where c.relkind in ('r', 'p') and n.oid = c.relnamespace and n.nspname not in ('pg_catalog', 'information_schema') " +
		"order by c.relname"
}

func (r *Grammar) CompileTypes() string {
	return `select t.typname as name, n.nspname as schema, t.typtype as type, t.typcategory as category, 
		((t.typinput = 'array_in'::regproc and t.typoutput = 'array_out'::regproc) or t.typtype = 'm') as implicit 
		from pg_type t 
		join pg_namespace n on n.oid = t.typnamespace 
		left join pg_class c on c.oid = t.typrelid 
		left join pg_type el on el.oid = t.typelem 
		left join pg_class ce on ce.oid = el.typrelid 
		where ((t.typrelid = 0 and (ce.relkind = 'c' or ce.relkind is null)) or c.relkind = 'c') 
		and not exists (select 1 from pg_depend d where d.objid in (t.oid, t.typelem) and d.deptype = 'e') 
		and n.nspname not in ('pg_catalog', 'information_schema')`
}

func (r *Grammar) CompileUnique(blueprint contractsschema.Blueprint, command *contractsschema.Command) string {
	sql := fmt.Sprintf("alter table %s add constraint %s unique (%s)",
		r.wrap.Table(blueprint.GetTableName()),
		r.wrap.Column(command.Index),
		r.wrap.Columnize(command.Columns))

	if command.Deferrable != nil {
		if *command.Deferrable {
			sql += " deferrable"
		} else {
			sql += " not deferrable"
		}
	}
	if command.Deferrable != nil && command.InitiallyImmediate != nil {
		if *command.InitiallyImmediate {
			sql += " initially immediate"
		} else {
			sql += " initially deferred"
		}
	}

	return sql
}

func (r *Grammar) CompileViews(database string) string {
	return "select viewname as name, schemaname as schema, definition from pg_views where schemaname not in ('pg_catalog', 'information_schema') order by viewname"
}

func (r *Grammar) EscapeNames(names []string) []string {
	escapedNames := make([]string, 0, len(names))

	for _, name := range names {
		segments := strings.Split(name, ".")
		for i, segment := range segments {
			segments[i] = strings.Trim(segment, `'"`)
		}
		escapedName := `"` + strings.Join(segments, `"."`) + `"`
		escapedNames = append(escapedNames, escapedName)
	}

	return escapedNames
}

func (r *Grammar) GetAttributeCommands() []string {
	return r.attributeCommands
}

func (r *Grammar) ModifyDefault(blueprint contractsschema.Blueprint, column contractsschema.ColumnDefinition) string {
	if column.IsChange() {
		if column.GetAutoIncrement() {
			return ""
		}
		if column.GetDefault() != nil {
			return fmt.Sprintf(" set default %s", schema.ColumnDefaultValue(column.GetDefault()))
		}
		return " drop default"
	}
	if column.GetDefault() != nil {
		return fmt.Sprintf(" default %s", schema.ColumnDefaultValue(column.GetDefault()))
	}

	return ""
}

func (r *Grammar) ModifyNullable(blueprint contractsschema.Blueprint, column contractsschema.ColumnDefinition) string {
	if column.IsChange() {
		if column.GetNullable() {
			return " drop not null"
		}
		return " set not null"
	}
	if column.GetNullable() {
		return " null"
	}
	return " not null"
}

func (r *Grammar) ModifyIncrement(blueprint contractsschema.Blueprint, column contractsschema.ColumnDefinition) string {
	if !column.IsChange() && !blueprint.HasCommand("primary") && slices.Contains(r.serials, column.GetType()) && column.GetAutoIncrement() {
		return " primary key"
	}

	return ""
}

func (r *Grammar) TypeBigInteger(column contractsschema.ColumnDefinition) string {
	if column.GetAutoIncrement() {
		return "bigserial"
	}

	return "bigint"
}

func (r *Grammar) TypeBoolean(column contractsschema.ColumnDefinition) string {
	return "boolean"
}

func (r *Grammar) TypeChar(column contractsschema.ColumnDefinition) string {
	length := column.GetLength()
	if length > 0 {
		return fmt.Sprintf("char(%d)", length)
	}

	return "char"
}

func (r *Grammar) TypeDate(column contractsschema.ColumnDefinition) string {
	return "date"
}

func (r *Grammar) TypeDateTime(column contractsschema.ColumnDefinition) string {
	return r.TypeTimestamp(column)
}

func (r *Grammar) TypeDateTimeTz(column contractsschema.ColumnDefinition) string {
	return r.TypeTimestampTz(column)
}

func (r *Grammar) TypeDecimal(column contractsschema.ColumnDefinition) string {
	return fmt.Sprintf("decimal(%d, %d)", column.GetTotal(), column.GetPlaces())
}

func (r *Grammar) TypeDouble(column contractsschema.ColumnDefinition) string {
	return "double precision"
}

func (r *Grammar) TypeEnum(column contractsschema.ColumnDefinition) string {
	return fmt.Sprintf(`varchar(255) check ("%s" in (%s))`, column.GetName(), strings.Join(r.wrap.Quotes(cast.ToStringSlice(column.GetAllowed())), ", "))
}

func (r *Grammar) TypeFloat(column contractsschema.ColumnDefinition) string {
	precision := column.GetPrecision()
	if precision > 0 {
		return fmt.Sprintf("float(%d)", precision)
	}

	return "float"
}

func (r *Grammar) TypeInteger(column contractsschema.ColumnDefinition) string {
	if column.GetAutoIncrement() {
		return "serial"
	}

	return "integer"
}

func (r *Grammar) TypeJson(column contractsschema.ColumnDefinition) string {
	return "json"
}

func (r *Grammar) TypeJsonb(column contractsschema.ColumnDefinition) string {
	return "jsonb"
}

func (r *Grammar) TypeLongText(column contractsschema.ColumnDefinition) string {
	return "text"
}

func (r *Grammar) TypeMediumInteger(column contractsschema.ColumnDefinition) string {
	return r.TypeInteger(column)
}

func (r *Grammar) TypeMediumText(column contractsschema.ColumnDefinition) string {
	return "text"
}

func (r *Grammar) TypeSmallInteger(column contractsschema.ColumnDefinition) string {
	if column.GetAutoIncrement() {
		return "smallserial"
	}

	return "smallint"
}

func (r *Grammar) TypeString(column contractsschema.ColumnDefinition) string {
	length := column.GetLength()
	if length > 0 {
		return fmt.Sprintf("varchar(%d)", length)
	}

	return "varchar"
}

func (r *Grammar) TypeText(column contractsschema.ColumnDefinition) string {
	return "text"
}

func (r *Grammar) TypeTime(column contractsschema.ColumnDefinition) string {
	return fmt.Sprintf("time(%d) without time zone", column.GetPrecision())
}

func (r *Grammar) TypeTimeTz(column contractsschema.ColumnDefinition) string {
	return fmt.Sprintf("time(%d) with time zone", column.GetPrecision())
}

func (r *Grammar) TypeTimestamp(column contractsschema.ColumnDefinition) string {
	if column.GetUseCurrent() {
		column.Default(schema.Expression("CURRENT_TIMESTAMP"))
	}

	return fmt.Sprintf("timestamp(%d) without time zone", column.GetPrecision())
}

func (r *Grammar) TypeTimestampTz(column contractsschema.ColumnDefinition) string {
	if column.GetUseCurrent() {
		column.Default(schema.Expression("CURRENT_TIMESTAMP"))
	}

	return fmt.Sprintf("timestamp(%d) with time zone", column.GetPrecision())
}

func (r *Grammar) TypeTinyInteger(column contractsschema.ColumnDefinition) string {
	return r.TypeSmallInteger(column)
}

func (r *Grammar) TypeTinyText(column contractsschema.ColumnDefinition) string {
	return "varchar(255)"
}

func (r *Grammar) getColumns(blueprint contractsschema.Blueprint) []string {
	var columns []string
	for _, column := range blueprint.GetAddedColumns() {
		columns = append(columns, r.getColumn(blueprint, column))
	}

	return columns
}

func (r *Grammar) getColumn(blueprint contractsschema.Blueprint, column contractsschema.ColumnDefinition) string {
	sql := fmt.Sprintf("%s %s", r.wrap.Column(column.GetName()), schema.ColumnType(r, column))

	for _, modifier := range r.modifiers {
		sql += modifier(blueprint, column)
	}

	return sql
}

func parseSchemaAndTable(reference, defaultSchema string) (string, string, error) {
	if reference == "" {
		return "", "", errors.SchemaEmptyReferenceString
	}

	parts := strings.Split(reference, ".")
	if len(parts) > 2 {
		return "", "", errors.SchemaErrorReferenceFormat
	}

	schema := defaultSchema
	if len(parts) == 2 {
		schema = parts[0]
		parts = parts[1:]
	}

	table := parts[0]

	return schema, table, nil
}
