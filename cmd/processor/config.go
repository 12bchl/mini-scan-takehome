package main

import (
	"flag"
	"os"
)

type Config struct {
	ProjectID      string
	SubscriptionID string
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

	flag.Parse()

	return cfg
}

func getEnv(key, dflt string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return dflt
}
