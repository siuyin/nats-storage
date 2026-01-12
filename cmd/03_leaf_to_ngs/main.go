package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/siuyin/nats-storage/nstor"
)

func main() {
	nc := connectToLeaf()
	defer nc.Close()

	ts := timeService(nc)
	defer ts.Unsubscribe()

	start := time.Now()
	msg, err := nc.Request("time", []byte{}, 10*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("sent request for time to leaf node, received: %s (%d microseconds)\n", string(msg.Data), time.Now().Sub(start).Microseconds())

	ng := connectToNGS()
	defer ng.Close()

	start = time.Now()
	msg, err = ng.Request("time", []byte{}, 200*time.Millisecond)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("sent request for time NGS, received: %s (%d microseconds)\n", string(msg.Data), time.Now().Sub(start).Microseconds())

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	lstrm, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "lstrm", Subjects: []string{"ord.>"}, MaxAge: 10 * time.Minute, MaxBytes: 1000000})

	jg, err := jetstream.New(ng)
	if err != nil {
		log.Fatal(err)
	}

	leafSrc := &jetstream.StreamSource{Name: "lstrm", Domain: "leaf"}
	gstrm, err := jg.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "gstrm", MaxAge: 10 * time.Minute, MaxBytes: 1000000, Sources: []*jetstream.StreamSource{leafSrc},
	})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := js.Publish(ctx, "ord.app", []byte(time.Now().Format("15:04:05.000000 -0700"))); err != nil {
		log.Fatal(err)
	}
	logStreamInfo(lstrm)
	logStreamInfo(gstrm)

	lc, err := lstrm.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "lcons", DeliverPolicy: jetstream.DeliverLastPolicy})
	if err != nil {
		log.Fatal(err)
	}

	gc, err := gstrm.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "gcons", DeliverPolicy: jetstream.DeliverLastPolicy})
	if err != nil {
		log.Fatal(err)
	}

	printConsumption(lc)
	printConsumption(gc)

	log.Println("ending run")
}

func connectToLeaf() *nats.Conn {
	nc, err := nats.Connect(nats.DefaultURL) // connects to the leaf node
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected to leaf node locally")
	return nc
}

func timeService(nc *nats.Conn) *nats.Subscription {
	sub, err := nc.Subscribe("time", func(m *nats.Msg) {
		m.Respond([]byte(time.Now().Format("15:04:05.0000 -0700")))
	})
	if err != nil {
		log.Fatal(err)
	}
	return sub
}

func connectToNGS() *nats.Conn {
	ng, err := nstor.Connect()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected to NGS")
	return ng
}

func logStreamInfo(strm jetstream.Stream) {
	ctx := context.Background()

	strmInfo, err := strm.Info(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%s has %d messages\n", strmInfo.Config.Name, strmInfo.State.Msgs)
}

func printConsumption(c jetstream.Consumer) {
	ctx := context.Background()
	cInfo, err := c.Info(ctx)
	if err != nil {
		log.Fatal(err)
	}

	iter, err := c.Messages()
	maxWait := jetstream.NextMaxWait(300 * time.Millisecond)
	for {
		msg, err := iter.Next(maxWait)
		if err != nil && err.Error() == "nats: timeout" {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %s\n", cInfo.Name, string(msg.Data()))
		msg.Ack()
	}

}
