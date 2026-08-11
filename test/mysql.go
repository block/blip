package test

import (
	"database/sql"
	"fmt"
	"os"
)

// Build is true when running in GitHub Actions. When true, database tests are
// skipped because currently we don't run MySQL in GitHub Acitons, but other tests
// and the Go build still run.
var Build = os.Getenv("GITHUB_ACTION") != ""

// DefaultMySQLVersion is used in all tests, corresponds to a MySQLPort value.
// To make a specify test use a different MySQL version, set it explicitly in
// the test, like test.Connection("<specific version>"). Make sure it has a key
// in MySQLPort.
var DefaultMySQLVersion = "mysql80"

// MySQLPort maps to Docker ports in docker/docker-compose.yaml.
var MySQLPort = map[string]string{
	"mysql80": "33800",
	"mysql84": "33840",
	"mysql57": "33570",
	"ps57":    "33900",
}

func Connection(distroVersion string) (string, *sql.DB, error) {
	port, ok := MySQLPort[distroVersion]
	if !ok {
		return "", nil, fmt.Errorf("invalid distro-version: %s (see dockerPorts in test/mysql.go)", distroVersion)
	}
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/?parseTime=true&interpolateParams=true",
		"root",
		"test",
		"127.0.0.1",
		port,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return "", nil, err
	}
	return dsn, db, nil
}
