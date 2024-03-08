package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/lib/pq"
)

var pool *sql.DB

func Close() {
	if pool != nil {
		pool.Close()
	}
}

func NewConection(serviceURI string) {
	var err error
	conn, _ := url.Parse(serviceURI)
	conn.RawQuery = "sslmode=verify-ca;sslrootcert=ca.pem"

	pool, err = sql.Open("postgres", conn.String())
	if err != nil {
		log.Fatal("unable to use data source name", err)
	}

	pool.SetConnMaxLifetime(0)
	pool.SetMaxIdleConns(3)
	pool.SetMaxOpenConns(3)

	ctx, stop := context.WithCancel(context.Background())

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		<-appSignal
		stop()
	}()

	Ping(ctx)
	Query()
}

func Ping(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := pool.PingContext(ctx); err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
}

func Query() {
	rows, err := pool.Query("SELECT version()")
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var result string
		err = rows.Scan(&result)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Version: %s\n", result)
	}
}

func Notify(channel, payload string) {
	stmt, err := pool.Prepare(fmt.Sprintf("NOTIFY %s, '%s'", channel, payload))
	if err != nil {
		panic(err)
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}
}

func Listen(dsn, channel string) {
	NewConection(dsn)
	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	minReconn := 10 * time.Second
	maxReconn := time.Minute
	listener := pq.NewListener(dsn, minReconn, maxReconn, reportProblem)
	err := listener.Listen(channel)
	if err != nil {
		panic(err)
	}

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)
	ctx, stop := context.WithCancel(context.Background())

	go func() {
		<-appSignal
		stop()
		listener.Close()
	}()
	waitForNotification(listener, ctx)
}

func waitForNotification(l *pq.Listener, ctx context.Context) {
	for {
		select {
		case p := <-l.Notify:
			log.Printf("received notification, %s\n", p.Channel)
			log.Println(p.Extra)
		case <-time.After(90 * time.Second):
			go l.Ping()
		case <-ctx.Done():
			return
		}
	}
}
