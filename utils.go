package postgres

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"os/exec"
	"reflect"
	"strings"
	"unicode"

	"github.com/spf13/cast"

	"github.com/goravel/framework/contracts/database/schema"
	"github.com/goravel/framework/contracts/testing"
	"github.com/goravel/framework/errors"
	"github.com/goravel/framework/support/str"
)

type Expression string

// Used by TestContainer, to simulate the port is using.
var testPortUsing = false

func isPortUsing(port int) bool {
	if testPortUsing {
		return true
	}

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if l != nil {
		l.Close()
	}

	return err != nil
}

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

func getExposedPort(exposedPorts []string, port int) int {
	for _, exposedPort := range exposedPorts {
		if !strings.Contains(exposedPort, cast.ToString(port)) {
			continue
		}

		ports := strings.Split(exposedPort, ":")

		return cast.ToInt(ports[0])
	}

	return 0
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

func getValidPort() int {
	for i := 0; i < 60; i++ {
		random := rand.Intn(10000) + 10000
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", random))
		if err != nil {
			continue
		}
		defer l.Close()

		return random
	}

	return 0
}

func imageToCommand(image *testing.Image) (command string, exposedPorts []string) {
	if image == nil {
		return "", nil
	}

	commands := []string{"docker", "run", "--rm", "-d"}
	if len(image.Env) > 0 {
		for _, env := range image.Env {
			commands = append(commands, "-e", env)
		}
	}
	var ports []string
	if len(image.ExposedPorts) > 0 {
		for _, port := range image.ExposedPorts {
			if !strings.Contains(port, ":") {
				port = fmt.Sprintf("%d:%s", getValidPort(), port)
			}
			ports = append(ports, port)
			commands = append(commands, "-p", port)
		}
	}

	commands = append(commands, fmt.Sprintf("%s:%s", image.Repository, image.Tag))

	return strings.Join(commands, " "), ports
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

func run(command string) (string, error) {
	cmd := exec.Command("/bin/sh", "-c", command)

	var out bytes.Buffer
	var stderr bytes.Buffer

	cmd.Stdout = &out
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s: %s", err, stderr.String())
	}

	return str.Of(out.String()).Squish().String(), nil
}
