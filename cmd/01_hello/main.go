package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/siuyin/nats-storage/nstor"
)

func main() {
	nc, err := nstor.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("NATS Connected")

	ctx := context.Background()
	kv, err := nstor.CreateKeyValue(ctx, nc, "mykv", 1000000)
	if err != nil {
		log.Fatal(err)
	}

	kv.Put(ctx, "gerbau", []byte(fmt.Sprintf("terpau: %s", time.Now().Format("2006-01-02 15:04:05.000 -0700"))))

	entry, err := kv.Get(ctx, "gerbau")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s (rev: %d) -> %q\n", entry.Key(), entry.Revision(), string(entry.Value()))
}
