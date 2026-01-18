# Mini-Scan

This project implements a scan processor at: [`cmd/processor/main.go`](cmd/processor/main.go)

Processor configuration defined in: [`cmd/processor/config.go`](cmd/processor/config.go)

Scan deserialization logic lives in: [`internal/model/types.go`](internal/model/types.go)

Supported SQL storage drivers:
- `sqlite3`
- `postgres`

### Usage

Run the processor (2 consumer example):

```bash
docker compose up --build --scale processor=2
```

Run storage controller tests (SQLite only):

```bash
go test -count=1 ./internal/storage
```
