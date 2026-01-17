package storage

import (
	"database/sql"

	"github.com/censys/scan-takehome/internal/model"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteScanStore(path string) (ScanStore, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	db.Exec(`PRAGMA journal_mode=WAL;`) // enable concurrent reads on db

	db.SetMaxOpenConns(1) // disable concurrent writes on client

	schema := `
    CREATE TABLE IF NOT EXISTS scans (
        ip TEXT NOT NULL,
        port INTEGER NOT NULL,
        service TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        response TEXT NOT NULL,
        PRIMARY KEY (ip, port, service)
    );`

	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}

	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Upsert(scan model.ScanResult) error {
	_, err := s.db.Exec(`
        INSERT INTO scans (ip, port, service, timestamp, response)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(ip, port, service) DO UPDATE SET
            timestamp = excluded.timestamp,
            response = excluded.response
        WHERE excluded.timestamp > scans.timestamp
    `,
		scan.Ip,
		scan.Port,
		scan.Service,
		scan.Timestamp,
		scan.Response,
	)
	return err
}
