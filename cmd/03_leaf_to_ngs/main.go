package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/siuyin/nats-storage/nstor"
)

func main() {
	nc := connectToLeaf()

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
