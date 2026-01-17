package main

import (
	"flag"
	"os"

	"github.com/censys/scan-takehome/internal/storage"
)

// NOTE: could switch to urfave/cli in future
type Config struct {
	ProjectID      string
	SubscriptionID string
	Storage        storage.Config
}

func LoadConfig() Config {
	cfg := Config{}

	flag.StringVar(
		&cfg.ProjectID,
		"project",
		getEnv("PUBSUB_PROJECT_ID", "test-project"),
		"GCP Project ID",
	)

	flag.StringVar(
		&cfg.SubscriptionID,
		"subscription",
		getEnv("PUBSUB_SUBSCRIPTION_ID", "scan-sub"),
		"GCP Subscription ID",
	)

	cfg.Storage = storage.LoadConfig()

	flag.Parse()

	return cfg
}

func getEnv(key, dflt string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return dflt
}
