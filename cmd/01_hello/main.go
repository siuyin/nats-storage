package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/siuyin/nats-storage/nstor"
)

func main() {
	fmt.Printf("start: %s\n", timestamp())
	nc, err := nstor.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	fmt.Printf("connected: %s\n", timestamp())

	log.Println("NATS Connected")

	ctx := context.Background()
	kv, err := nstor.CreateKeyValue(ctx, nc, "mykv", 1000000)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("KV inited: %s\n", timestamp())
	key := "merbau"
	kv.Put(ctx, key, []byte(fmt.Sprintf("terpau: %s", time.Now().Format("2006-01-02 15:04:05.000 -0700"))))
	fmt.Printf("KV put: %s\n", timestamp())

	entry, err := kv.Get(ctx, key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("KV get: %s\n", timestamp())

	fmt.Printf("%s (rev: %d) -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))
}

func timestamp() string {
	return time.Now().Format("15:04:05.000000")
}
