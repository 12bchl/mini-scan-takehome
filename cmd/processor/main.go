package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/censys/scan-takehome/internal/model"
	"github.com/censys/scan-takehome/internal/storage"

	"cloud.google.com/go/pubsub"
)

func main() {
	cfg := LoadConfig()
	log.Printf("processor configured: %v", cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		log.Fatalf("failed to create pub-sub client: %v", err)
	}
	defer client.Close()

	sub := client.Subscription(cfg.SubscriptionID)

	store, err := storage.NewScanStore(cfg.Storage)
	if err != nil {
		log.Fatalf("failed to create scan storage: %v", err)
	}
	defer store.Close()

	if err := store.Connect(ctx); err != nil {
		log.Fatalf("failed to connect storage: %v", err)
	}

	log.Println("processor started")

	// TODO: abstract msg handling to support alternate message queues
	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		log.Printf("subscription received (%d bytes)", len(msg.Data))

		var scan model.ScanResult
		if err := json.Unmarshal(msg.Data, &scan); err != nil {
			log.Printf("failed to parse scan result: err=%v msg='%s'", err, string(msg.Data))
			msg.Ack() // prevent retry
			return
		}

		if err := store.Upsert(ctx, scan); err != nil {
			log.Printf("failed to write to db: %v", err)
			return
		}

		// TODO: debug level logging
		log.Printf("susbscription processed: %v", scan)
		msg.Ack()
	})

	if err != nil {
		log.Fatalf("failed to receive on subscription: %v", err)
	}

	log.Println("processor stopped")
}
