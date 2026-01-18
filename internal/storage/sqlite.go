package storage

import (
	"context"

	"github.com/censys/scan-takehome/internal/model"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
	SQLScanStore
}

func (st *SQLiteStore) Connect(ctx context.Context) error {
	if err := st.SQLScanStore.Connect(ctx); err != nil {
		return err
	}
	return st.init(ctx)
}

func (st *SQLiteStore) init(ctx context.Context) error {
	st.db.SetMaxOpenConns(1)                                  // disable concurrent writes on client
	_, _ = st.db.ExecContext(ctx, `PRAGMA journal_mode=WAL;`) // enable concurrent reads on db

	schema := `
    CREATE TABLE IF NOT EXISTS scans (
        ip TEXT NOT NULL,
        port INTEGER NOT NULL,
        service TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        response TEXT NOT NULL,
        PRIMARY KEY (ip, port, service)
    );`

	_, err := st.db.ExecContext(ctx, schema)
	return err
}

func (st *SQLiteStore) Upsert(ctx context.Context, scan model.ScanResult) error {
	_, err := st.db.ExecContext(ctx, `
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
