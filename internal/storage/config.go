package storage

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"regexp"
)

// Config for sql scan stores
type Config struct {
	Driver string
	DSN    string
	Table  string
}

func LoadConfig() Config {
	cfg := Config{}

	flag.StringVar(
		&cfg.Driver,
		"driver",
		getEnv("STORAGE_SQL_DRIVER", "postgres"),
		// getEnv("STORAGE_SQL_DRIVER", "sqlite3"),
		"SQL Driver",
	)

	flag.StringVar(
		&cfg.DSN,
		"dsn",
		getEnv("STORAGE_SQL_DSN", "postgres://scans_user:scans_pass@postgres:5432/scans?sslmode=disable"),
		// getEnv("STORAGE_SQL_DSN", "scans.db"),
		"SQL Data Source",
	)

	flag.StringVar(
		&cfg.Table,
		"table",
		getEnv("STORAGE_SQL_TABLE", "scans"),
		"SQL Table",
	)

	flag.Parse()

	return cfg
}

func getEnv(key, dflt string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return dflt
}

func (cfg Config) newScanStore(ctx context.Context) (SQLScanStore, error) {
	if err := validateTable(cfg.Table); err != nil {
		return SQLScanStore{}, err
	}

	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return SQLScanStore{}, err
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return SQLScanStore{}, err
	}

	return SQLScanStore{db: db, table: cfg.Table}, nil
}

var identRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,62}$`)

func validateTable(table string) error {
	// check for invalid/unsafe characters
	if !identRe.MatchString(table) {
		return fmt.Errorf("invalid table name: %q", table)
	}
	return nil
}
