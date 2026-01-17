FROM golang:1.20-bookworm AS builder

# Build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .

# NOTE: cgo implementation of sqlite3 driver is more performant than non-cgo alternatives
RUN CGO_ENABLED=1 go build -o processor ./cmd/processor

# Copy binary into slim image
FROM debian:bookworm-slim
WORKDIR app
COPY --from=builder /src/processor .
CMD ["/app/processor"]
