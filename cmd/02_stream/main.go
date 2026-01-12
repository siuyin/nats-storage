package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
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

	_, myConsumer, err := createStreamAndConsumer(context.Background(), nc, "mystream", 1000000, []string{"mysubj"}, "mycons")
	if err != nil {
		log.Fatal(err)
	}

	publishSampleData(nc)

	receiveAndProcessMessages(myConsumer)

	log.Println("stream and consumer processing done")
}

func createStreamAndConsumer(ctx context.Context, nc *nats.Conn, streamName string, size int64, subjects []string, consumerName string) (jetstream.Stream, jetstream.Consumer, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, nil, fmt.Errorf("creating jetstream context: %v", err)
	}

	cfg := jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  subjects,
		MaxBytes:  size,
		MaxAge:    time.Hour,
		Retention: jetstream.WorkQueuePolicy,
	}
	s, err := js.CreateStream(context.Background(), cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating stream: %v", err)
	}

	c, err := s.CreateConsumer(context.Background(), jetstream.ConsumerConfig{Name: "mycons"})
	if err != nil {
		return nil, nil, fmt.Errorf("creating consumer: %v", err)
	}

	return s, c, nil
}

func publishSampleData(nc *nats.Conn) {
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}
	for n := 0; n < 3; n++ {
		if _, err := js.Publish(context.Background(), "mysubj", []byte(time.Now().Format("15:04:05.000000")),
			jetstream.WithMsgID(nuid.Next())); err != nil {
			log.Fatal(err)
		}
	}
}

func receiveAndProcessMessages(c jetstream.Consumer) {
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
}
