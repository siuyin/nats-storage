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

type leaf struct {
	nc *nats.Conn
	ns *server.Server
	js jetstream.JetStream // local JetStream context
	jr jetstream.JetStream // remote JetStream context
	kv jetstream.KeyValue
}

func newLeaf(ctx context.Context, name, source, srcDomain string) (*leaf, error) {
	var err error
	l := leaf{}
	l.nc, l.ns, err = embedNATSServer()
	if err != nil {
		return nil, fmt.Errorf("newLeaf: %v", err)
	}

	l.js, err = jetstream.New(l.nc)
	if err != nil {
		return nil, fmt.Errorf("newLeaf: jetstream: %v", err)
	}

	remDomain := dflt.EnvString("REMOTE_JETSTREAM_DOMAIN", "rasp")
	l.jr, err = jetstream.NewWithDomain(l.nc, remDomain)
	if err != nil {
		return nil, fmt.Errorf("newLeaf: jetstream with domain: %v", err)
	}

	l.kv, err = l.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: name + "kv", Mirror: &jetstream.StreamSource{Name: "KV_" + source + "kv", Domain: srcDomain}})
	if err != nil {
		return nil, fmt.Errorf("newLeaf create kv: %v", err)
	}

	return &l, nil
}

func (l *leaf) hiV1() {
	l.nc.Subscribe("v1.hi", func(m *nats.Msg) {
		m.Respond([]byte(fmt.Sprintf("Greetings %s: Nice to meet you. The time is %s.\n", string(m.Data), time.Now().Format("15:04:05.000 -0700"))))
	})
}

func (l *leaf) rstrm(ctx context.Context, name string, subs []string) (jetstream.Stream, error) {
	for {
		_, err := l.jr.AccountInfo(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}

	rstrm, err := l.jr.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: name, Subjects: subs, MaxAge: 10 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("leaf rstrm: create stream: %s: %v", name, err)
	}
	return rstrm, nil
}

func main() {
	lf, err := newLeaf(ctx, "my", "my", "rasp")
	if err != nil {
		log.Println(err)
		return
	}
	defer lf.nc.Close()
	defer lf.ns.WaitForShutdown() // requires a ctrl-C to terminate
	//defer lf.ns.Shutdown()

	lf.hiV1()

	ctx := context.Background()
	mstrm, err := lf.rstrm(ctx, "mstrm", []string{"m.>"})
	if err != nil {
		log.Println(err)
		return
	}

	_ = mstrm
	log.Println("remote stream mstrm created")
}

func embedNATSServer() (*nats.Conn, *server.Server, error) {
	dir := dflt.EnvString("STORE_DIR", "/home/siuyin/embedded_nats")
	passwd := dflt.EnvString("LEAF_PASSWD", "your leaf connection password here")
	host := dflt.EnvString("LEAF_HOST", "rasp.beyondbroadcast.com:8080")
	log.Printf("STORE_DIR=%s LEAF_PASSWD=%s LEAF_HOST=%s", dir, passwd[0:5]+"...", host)
	opts := &server.Options{JetStream: true, StoreDir: dir, JetStreamDomain: "leaf", Port: 4222, DontListen: false,
		LeafNode: server.LeafNodeOpts{Remotes: []*server.RemoteLeafOpts{
			&server.RemoteLeafOpts{URLs: []*url.URL{
				&url.URL{Scheme: "tls", Host: host, User: url.UserPassword("a", passwd)}},
			}},
		},
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, nil, err
	}

	ns.ConfigureLogger()
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
