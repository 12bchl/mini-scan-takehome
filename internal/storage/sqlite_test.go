package storage

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/censys/scan-takehome/internal/model"
)

func newTestSQLiteStore(t *testing.T) (*SQLiteStore, *sql.DB) {
	t.Helper()

	ctx := context.Background()

	// TODO: configure list of supported drivers, create tmp/sim dbs for testing
	cfg := Config{
		Driver: "sqlite3",
		DSN:    ":memory:",
		Table:  "scans",
	}

	storeIface, err := NewSQLiteScanStore(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}

	store := storeIface.(*SQLiteStore)
	return store, store.db
}

func TestSQLiteUpsert_OutOfOrderHandling(t *testing.T) {
	ctx := context.Background()
	store, db := newTestSQLiteStore(t)
	defer store.Close()

	base := model.ScanResult{
		ScanBase: model.ScanBase{
			Ip:      "1.2.3.4",
			Port:    443,
			Service: "https",
		},
	}

	tests := []struct {
		name      string
		timestamp int64
		response  string
		wantTS    int64
		wantResp  string
	}{
		{
			name:      "initial insert",
			timestamp: 100,
			response:  "first",
			wantTS:    100,
			wantResp:  "first",
		},
		{
			name:      "newer update wins",
			timestamp: 200,
			response:  "second",
			wantTS:    200,
			wantResp:  "second",
		},
		{
			name:      "older update ignored",
			timestamp: 150,
			response:  "stale",
			wantTS:    200,
			wantResp:  "second",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := base
			scan.Timestamp = tt.timestamp
			scan.Response = tt.response

			if err := store.Upsert(ctx, scan); err != nil {
				t.Fatalf("upsert failed: %v", err)
			}

			var gotTS int64
			var gotResp string

			row := db.QueryRow(`
				SELECT timestamp, response
				FROM scans
				WHERE ip = ? AND port = ? AND service = ?
			`,
				scan.Ip,
				scan.Port,
				scan.Service,
			)

			if err := row.Scan(&gotTS, &gotResp); err != nil {
				t.Fatalf("failed to read row: %v", err)
			}

			if gotTS != tt.wantTS {
				t.Fatalf("timestamp mismatch: got %d want %d", gotTS, tt.wantTS)
			}

			if gotResp != tt.wantResp {
				t.Fatalf("response mismatch: got %q want %q", gotResp, tt.wantResp)
			}
		})
	}
}

func TestSQLiteUpsert_ConcurrentWriters_MaxTimestampWins(t *testing.T) {
	ctx := context.Background()
	store, db := newTestSQLiteStore(t)
	defer store.Close()

	const (
		ip      = "10.0.0.1"
		port    = 443
		service = "https"
	)

	timestamps := []int64{10, 50, 30, 80, 20, 90, 40, 70, 60, 100}

	done := make(chan struct{}, len(timestamps))

	for _, timestamp := range timestamps {
		ts := timestamp
		go func() {
			scan := model.ScanResult{
				ScanBase: model.ScanBase{
					Ip:        ip,
					Port:      port,
					Service:   service,
					Timestamp: ts,
				},
				Response: "resp-" + fmt.Sprint(ts),
			}

			if err := store.Upsert(ctx, scan); err != nil {
				t.Errorf("upsert failed: %v", err)
			}
			done <- struct{}{}
		}()
	}

	for range timestamps {
		<-done
	}

	var gotTS int64
	var gotResp string

	row := db.QueryRow(`
		SELECT timestamp, response
		FROM scans
		WHERE ip = ? AND port = ? AND service = ?
	`, ip, port, service)

	if err := row.Scan(&gotTS, &gotResp); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if gotTS != 100 {
		t.Fatalf("expected max timestamp 100, got %d", gotTS)
	}

	if gotResp != "resp-100" {
		t.Fatalf("expected response resp-100, got %q", gotResp)
	}
}

func TestSQLiteUpsert_CrossPKIsolation(t *testing.T) {
	ctx := context.Background()
	store, db := newTestSQLiteStore(t)
	defer store.Close()

	scans := []model.ScanResult{
		{
			ScanBase: model.ScanBase{
				Ip:        "1.1.1.1",
				Port:      80,
				Service:   "http",
				Timestamp: 100,
			},
			Response: "http-1",
		},
		{
			ScanBase: model.ScanBase{
				Ip:        "3.3.3.3",
				Port:      80,
				Service:   "http",
				Timestamp: 100,
			},
			Response: "http-1",
		},
		{
			ScanBase: model.ScanBase{
				Ip:        "1.1.1.1",
				Port:      443,
				Service:   "https",
				Timestamp: 200,
			},
			Response: "https-1",
		},
		{
			ScanBase: model.ScanBase{
				Ip:        "2.2.2.2",
				Port:      443,
				Service:   "https",
				Timestamp: 300,
			},
			Response: "https-2",
		},
	}

	for _, scan := range scans {
		if err := store.Upsert(ctx, scan); err != nil {
			t.Fatalf("upsert failed: %v", err)
		}
	}

	rows, err := db.Query(`SELECT ip, port, service, timestamp, response FROM scans`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != len(scans) {
		t.Fatalf("expected %d rows, got %d", len(scans), count)
	}
}
