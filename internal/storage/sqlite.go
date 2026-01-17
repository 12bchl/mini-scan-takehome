package storage

import (
	"context"

	"github.com/censys/scan-takehome/internal/model"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
	SQLScanStore
}

func NewSQLiteScanStore(ctx context.Context, cfg Config) (ScanStore, error) {
	store, err := cfg.newScanStore(ctx)
	if err != nil {
		return nil, err
	}

	store.db.Exec(`PRAGMA journal_mode=WAL;`) // enable concurrent reads on db
	store.db.SetMaxOpenConns(1)               // disable concurrent writes on client

	schema := `
    CREATE TABLE IF NOT EXISTS scans (
        ip TEXT NOT NULL,
        port INTEGER NOT NULL,
        service TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        response TEXT NOT NULL,
        PRIMARY KEY (ip, port, service)
    );`

	if _, err := store.db.Exec(schema); err != nil {
		return nil, err
	}

	return &SQLiteStore{store}, nil
}

func (s *SQLiteStore) Upsert(ctx context.Context, scan model.ScanResult) error {
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO scans (ip, port, service, timestamp, response)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(ip, port, service)
				DO UPDATE SET
            timestamp = EXCLUDED.timestamp,
            response = EXCLUDED.response
        WHERE EXCLUDED.timestamp > scans.timestamp
    `,
		scan.Ip,
		scan.Port,
		scan.Service,
		scan.Timestamp,
		scan.Response,
	)
	return err
}
