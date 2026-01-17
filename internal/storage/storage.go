package storage

import "github.com/censys/scan-takehome/internal/model"

type ScanStore interface {
	Upsert(model.ScanResult) error
}

func NewScanStore(dbName string) (ScanStore, error) {
	return NewSQLiteScanStore(dbName)
}
