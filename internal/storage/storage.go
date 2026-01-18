package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/censys/scan-takehome/internal/model"
)

// ScanStore storage controller
type ScanStore interface {
	Connect(context.Context) error
	Upsert(context.Context, model.ScanResult) error
	Close() error
}

func NewScanStore(cfg Config) (_ ScanStore, err error) {
	store, err := cfg.newScanStore()
	if err != nil {
		return nil, err
	}

	// TODO: switch to sql driver "Registry" semantic
	switch cfg.Driver {
	case "sqlite3":
		return &SQLiteStore{store}, nil

	case "postgres":
		return &PostgresStore{store}, nil

	default:
		return nil, fmt.Errorf("unsupported sql driver: %s", cfg.Driver) // redundant
	}
}

// SQLScanStore common functionality
type SQLScanStore struct {
	db    *sql.DB
	table string
}

const ConnectWait = 2 * time.Second

func (s *SQLScanStore) Connect(ctx context.Context) error {
	ticker := time.NewTicker(ConnectWait)
	defer ticker.Stop()

	for {
		if err := s.db.PingContext(ctx); err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("database not ready; waiting %s...", ConnectWait)
		}
	}
}

func (s *SQLScanStore) Close() error {
	return s.db.Close()
}
