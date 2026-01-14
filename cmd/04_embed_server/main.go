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

type leaf1 struct {
	nc   *nats.Conn
	ns   *server.Server
	js   jetstream.JetStream
	strm jetstream.Stream
	cons jetstream.Consumer
	kv   jetstream.KeyValue
}

func newLeaf1(ctx context.Context, name string) *leaf1 {
	var err error
	l := leaf1{}
	l.nc, l.ns, err = embedNATSServer()
	if err != nil {
		log.Fatal("newLeaf1: ", err)
	}

	l.js, err = jetstream.New(l.nc)
	if err != nil {
		log.Fatal("newLeaf1: jetstream ", err)
	}

	l.strm, err = l.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: name, Subjects: []string{"m.>"}, MaxAge: 10 * time.Minute,
	})
	if err != nil {
		log.Fatal("newLeaf1: create stream: ", err)
	}

	l.cons, err = l.strm.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: name + "Cons"})
	if err != nil {
		log.Fatal("newLeaf1: create cons: ", err)
	}

	l.kv, err = l.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name + "KV"})
	if err != nil {
		log.Fatal("newLeaf1: create KV: ", err)
	}

	return &l
}

func (l *leaf1) receiveAndDisplayMessages(ctx context.Context) {
	iter, err := l.cons.Messages()
	if err != nil {
		log.Fatal("leaf1 recv: ", err)
	}

	maxWait := jetstream.NextMaxWait(10 * time.Millisecond)
	cInf, err := l.cons.Info(ctx)
	if err != nil {
		log.Fatal("leaf1 recv info : ", err)
	}
	for {
		msg, err := iter.Next(maxWait)
		if err != nil && err.Error() == "nats: timeout" {
			break
		}
		if err != nil {
			log.Fatal("leaf1 recv next: ", err)
		}

		fmt.Println(cInf.Name, ":", string(msg.Data()))
		msg.Ack()
	}
}

type hub struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	strm jetstream.Stream
	cons jetstream.Consumer
	kv   jetstream.KeyValue
}

func newHub(ctx context.Context, name, source, srcDomain string) *hub {
	var err error
	h := hub{}
	h.nc, err = nats.Connect(dflt.EnvString("NATS_URL", "a@localhost:4222"))
	if err != nil {
		log.Fatal("hub connect: ", err)
	}

	h.js, err = jetstream.New(h.nc)
	if err != nil {
		log.Fatal("hub jetstream: ", err)
	}

	h.strm, err = h.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: name, MaxAge: 10 * time.Minute,
		Sources: []*jetstream.StreamSource{&jetstream.StreamSource{Name: source, Domain: srcDomain}},
	})
	if err != nil {
		log.Fatal("hub create stream: ", err)
	}

	h.cons, err = h.strm.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: name + "Cons"})
	if err != nil {
		log.Fatal("hub create consumer: ", err)
	}

	h.kv, err = h.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name + "KV", Mirror: &jetstream.StreamSource{Name: "KV_" + source + "KV", Domain: srcDomain}})
	if err != nil {
		log.Fatal("hub create kv: ", err)
	}

	return &h
}

func (h *hub) receiveAndDisplayMessages(ctx context.Context) {
	iter, err := h.cons.Messages()
	if err != nil {
		log.Fatal("hub messages: ", err)
	}

	maxWait := jetstream.NextMaxWait(100 * time.Millisecond)
	cInf, err := h.cons.Info(ctx)
	if err != nil {
		log.Fatal("hub recv info : ", err)
	}
	for {
		msg, err := iter.Next(maxWait)
		if err != nil && err.Error() == "nats: timeout" {
			break
		}
		if err != nil {
			log.Fatal("hub recv next: ", err)
		}

		fmt.Println(cInf.Name, ":", string(msg.Data()))
		msg.Ack()
	}
}

func (h *hub) sync(ctx context.Context, l *leaf1) {
	start := time.Now()
	for {
		leafInf, err := l.strm.Info(ctx)
		if err != nil {
			log.Fatal("leaf1 info:", err)
		}

		hubInf, err := h.strm.Info(ctx)
		if err != nil {
			log.Fatal("hub info:", err)
		}

		if leafInf.State.LastTime.Sub(hubInf.State.LastTime) > 0 { // not caught up yet
			time.Sleep(10 * time.Millisecond)
			continue
		}
		break
	}

	log.Printf("sync took %v ms\n", time.Now().Sub(start).Milliseconds())
}

func main() {
	ctx := context.Background()
	lf := newLeaf1(ctx, "mstrm")
	defer lf.nc.Close()
	//defer lf.ns.WaitForShutdown() // requires a ctrl-C to terminate
	defer lf.ns.Shutdown()

	count, err := dflt.EnvInt("COUNT", 3)
	log.Printf("COUNT=%d", count)
	if err != nil {
		log.Fatal("count: ", err)
	}

	for i := 0; i < count; i++ {
		if _, err := lf.js.Publish(ctx, "m.1", []byte(time.Now().Format("15:04:05.000000 -0700"))); err != nil {
			log.Println("stream publish: ", err)
		}
	}

	lf.receiveAndDisplayMessages(ctx)

	hb := newHub(ctx, "lmstrm", "mstrm", "leaf1")
	defer hb.nc.Close()

	hb.sync(ctx, lf)

	hb.receiveAndDisplayMessages(ctx)
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
