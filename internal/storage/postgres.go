package storage

import (
	"context"

	"github.com/censys/scan-takehome/internal/model"
	_ "github.com/lib/pq"
)

type PostgresStore struct {
	SQLScanStore
}

func (st *PostgresStore) Connect(ctx context.Context) error {
	if err := st.SQLScanStore.Connect(ctx); err != nil {
		return err
	}
	return st.init(ctx)
}

func (st *PostgresStore) init(ctx context.Context) error {
	// TODO: schema validation if table already exists
	schema := `
	CREATE TABLE IF NOT EXISTS scans (
		ip TEXT NOT NULL,
		port INTEGER NOT NULL,
		service TEXT NOT NULL,
		timestamp BIGINT NOT NULL,
		response TEXT NOT NULL,
		PRIMARY KEY (ip, port, service)
	);`

	_, err := st.db.ExecContext(ctx, schema)
	return err
}

func (st *PostgresStore) Upsert(ctx context.Context, scan model.ScanResult) error {
	_, err := st.db.ExecContext(ctx, `
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
