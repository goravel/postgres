package postgres

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/spf13/cast"

	"github.com/goravel/framework/contracts/database/schema"
	"github.com/goravel/framework/errors"
)

type Expression string

func getDefaultValue(def any) string {
	switch value := def.(type) {
	case bool:
		return "'" + cast.ToString(cast.ToInt(value)) + "'"
	case Expression:
		return string(value)
	default:
		return "'" + cast.ToString(def) + "'"
	}
}

func getType(grammar schema.Grammar, column schema.ColumnDefinition) string {
	t := []rune(column.GetType())
	if len(t) == 0 {
		return ""
	}

	t[0] = unicode.ToUpper(t[0])
	methodName := fmt.Sprintf("Type%s", string(t))
	methodValue := reflect.ValueOf(grammar).MethodByName(methodName)
	if methodValue.IsValid() {
		args := []reflect.Value{reflect.ValueOf(column)}
		callResult := methodValue.Call(args)

		return callResult[0].String()
	}

	return ""
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
