package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
	"github.com/siuyin/nats-storage/nstor"
)

func main() {
	nc, err := nstor.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	cfg := jetstream.StreamConfig{
		Name:      "mystream",
		Subjects:  []string{"mysubj"},
		MaxBytes:  1000000,
		MaxAge:    time.Hour,
		Retention: jetstream.WorkQueuePolicy,
	}
	s, err := js.CreateStream(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}

	//c, err := s.CreateConsumer(context.Background(), jetstream.ConsumerConfig{Name: "mycons", FilterSubject: "mysubj"})
	c, err := s.CreateConsumer(context.Background(), jetstream.ConsumerConfig{Name: "mycons-1"})
	if err != nil {
		log.Fatal(err)
	}

	for n := 0; n < 3; n++ {
		if _, err := js.Publish(context.Background(), "mysubj", []byte(time.Now().Format("15:04:05.000000")),
			jetstream.WithMsgID(nuid.Next())); err != nil {
			log.Fatal(err)
		}
	}

	msgIter, err := c.Messages()
	if err != nil {
		log.Fatal(err)
	}
loop:
	for {
		msg, err := msgIter.Next(jetstream.NextMaxWait(80 * time.Millisecond))
		switch {
		case err != nil && err.Error() == "nats: timeout":
			break loop
		case err != nil:
			log.Fatal(err)
		}
		fmt.Printf("%s\n", msg.Data())
		msg.Ack() // must explicitly ack.
	}

	log.Println("stream and consumer created")
}
