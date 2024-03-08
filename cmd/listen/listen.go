package main

import (
	"log"
	"os"

	"github.com/nenodias/pg-go-pubsub/internal/db"
)

func main() {
	NewListener()
}

func NewListener() {
	dsn := os.Getenv("DB_DSN")
	channel := os.Getenv("DB_CHANNEL")
	log.Printf("Connect with [%s]\n -> %s\n", dsn, channel)
	db.Listen(dsn, channel)
	defer db.Close()
}
