package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/siuyin/dflt"
)

var ctx = context.Background()

func main() {
	nc, ns, err := embedNATSServer()
	if err != nil {
		log.Fatal(err)
	}
	//defer ns.WaitForShutdown() // requires a ctrl-C to terminate
	defer ns.Shutdown()
	defer nc.Close()
	log.Println("server started")

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if _, err := js.Publish(ctx, "m.1", []byte(time.Now().Format("15:04:05.000000 -0700"))); err != nil {
			log.Fatal(err)
		}
	}

	demoStream(js, "mstrm")

	nl, err := nats.Connect("a@localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nl.Close()

	jl, err := jetstream.New(nl)
	if err != nil {
		log.Fatal(err)
	}

	demoSourceStream(jl, "lmstrm", "mstrm", "leaf1")

}

func embedNATSServer() (*nats.Conn, *server.Server, error) {
	dir := dflt.EnvString("STORE_DIR", "/home/siuyin/embedded_nats")
	log.Printf("STORE_DIR=%s", dir)
	opts := &server.Options{ServerName: "leaf1", JetStream: true, StoreDir: dir, JetStreamDomain: "leaf1", Port: 4223, DontListen: false,
		LeafNode: server.LeafNodeOpts{Remotes: []*server.RemoteLeafOpts{
			&server.RemoteLeafOpts{URLs: []*url.URL{
				&url.URL{Scheme: "leaf", Host: "127.0.0.1", User: url.UserPassword("a", "a")}},
			}},
		},
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, nil, err
	}

	//ns.ConfigureLogger()
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, nil, errors.New("could not start embedded NATS server within 5 seconds")
	}

	nc, err := nats.Connect(ns.ClientURL(), nats.InProcessServer(ns))
	if err != nil {
		return nil, nil, err
	}

	return nc, ns, nil
}

func demoStream(js jetstream.JetStream, name string) {
	strm, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: name, Subjects: []string{"m.>"}, MaxAge: 10 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}

	cons, err := strm.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: name + "Cons"})
	if err != nil {
		log.Fatal(err)
	}

	iter, err := cons.Messages()
	if err != nil {
		log.Fatal(err)
	}

	maxWait := jetstream.NextMaxWait(10 * time.Millisecond)
	for {
		msg, err := iter.Next(maxWait)
		if err != nil && err.Error() == "nats: timeout" {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(name, ":", string(msg.Data()))
		msg.Ack()
	}

}
func demoSourceStream(js jetstream.JetStream, name, source, srcDomain string) {
	strm, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: name, MaxAge: 10 * time.Minute, Sources: []*jetstream.StreamSource{
			&jetstream.StreamSource{Name: source, Domain: srcDomain}}})
	if err != nil {
		log.Fatal(err)
	}

	cons, err := strm.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: name + "Cons"})
	if err != nil {
		log.Fatal(err)
	}

	iter2, err := cons.Messages()
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond) // allow time for stream relication
	maxWait := jetstream.NextMaxWait(10 * time.Millisecond)
	for {
		msg, err := iter2.Next(maxWait)
		if err != nil && err.Error() == "nats: timeout" {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(name+":", string(msg.Data()))
		msg.Ack()
	}
}
