package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/censys/scan-takehome/internal/model"
	"github.com/censys/scan-takehome/internal/storage"

	"cloud.google.com/go/pubsub"
)

func main() {
	cfg := LoadConfig()

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		log.Fatal("Failed to create pub-sub client: ", err)
	}
	sub := client.Subscription(cfg.SubscriptionID)

	store, err := storage.NewScanStore("scans.db")
	if err != nil {
		log.Fatal("Failed to create scan storage: ", err)
	}

	log.Println("Processor started")

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		var scan model.ScanResult
		if err := json.Unmarshal(msg.Data, &scan); err != nil {
			log.Println("Failed to parse scan result", "msg", string(msg.Data), "error", err)
			msg.Ack()
			return
		}

		if err := store.Upsert(scan); err != nil {
			log.Println("Failed to write to db", "msg", string(msg.Data), "error", err)
			return
		}

		log.Println("Susbscription processed", "scan", scan)

		msg.Ack()
	})

	if err != nil {
		log.Fatal("Failed to receive on subscription: ", err)
	}
}
