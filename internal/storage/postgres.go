package storage

import (
	"context"

	"github.com/censys/scan-takehome/internal/model"
	_ "github.com/lib/pq"
)

type PostgresStore struct {
	SQLScanStore
}

func NewPostgresScanStore(ctx context.Context, cfg Config) (ScanStore, error) {
	store, err := cfg.newScanStore(ctx)
	if err != nil {
		return nil, err
	}

	schema := `
	CREATE TABLE IF NOT EXISTS scans (
		ip TEXT NOT NULL,
		port INTEGER NOT NULL,
		service TEXT NOT NULL,
		timestamp BIGINT NOT NULL,
		response TEXT NOT NULL,
		PRIMARY KEY (ip, port, service)
	);`

	if _, err := store.db.Exec(schema); err != nil {
		return nil, err
	}

	return &PostgresStore{store}, nil
}

func (p *PostgresStore) Upsert(ctx context.Context, scan model.ScanResult) error {
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO scans (ip, port, service, timestamp, response)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (ip, port, service)
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
