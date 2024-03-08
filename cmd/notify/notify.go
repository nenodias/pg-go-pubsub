package main

import (
	"log"
	"os"

	"github.com/nenodias/pg-go-pubsub/internal/db"
)

func main() {
	dsn := os.Getenv("DB_DSN")
	channel := os.Getenv("DB_CHANNEL")
	message := os.Getenv("DB_MESSAGE")
	log.Printf("Connect with [%s]\n -> %s(%s)\n", dsn, channel, message)
	db.NewConection(dsn)
	db.Notify(channel, message)
	defer db.Close()
}
