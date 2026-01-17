package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/censys/scan-takehome/internal/model"
)

// ScanStore storage controller
type ScanStore interface {
	Upsert(context.Context, model.ScanResult) error
	Close() error
}

func NewScanStore(ctx context.Context, cfg Config) (_ ScanStore, err error) {
	// TODO: switch to Registry semantic
	switch cfg.Driver {
	case "sqlite3":
		return NewSQLiteScanStore(ctx, cfg)

	case "postgres":
		return NewPostgresScanStore(ctx, cfg)

	default:
		return nil, fmt.Errorf("unsupported sql driver: %s", cfg.Driver)
	}
}

// SQLScanStore common functionality
type SQLScanStore struct {
	db    *sql.DB
	table string
}

func (s *SQLScanStore) Close() error {
	return s.db.Close()
}
