package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
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
	waitForLeafConnect(ctx, l)

	if _, err := l.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: name + l.Domain(), Subjects: subs, MaxAge: 10 * time.Minute}); err != nil {
		return nil, fmt.Errorf("rstrm: create local stream: %v", err)
	}

	rstrm, err := l.jr.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: name, MaxAge: 10 * time.Minute,
		Sources: []*jetstream.StreamSource{
			&jetstream.StreamSource{Name: name + l.Domain(), Domain: l.Domain()},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("leaf rstrm: create stream: %s: %v", name, err)
	}
	return rstrm, nil
}

func (l *leaf) rstrmSources(ctx context.Context, name string) ([]string, error) {
	s, err := l.jr.Stream(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("rstrmSources Stream: %v", err)
	}

	inf, err := s.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("rstrmSources info : %v", err)
	}
	srcs := []string{}
	for _, srcInf := range inf.Sources {
		srcs = append(srcs, srcInf.Name)
	}
	return srcs, nil
}

// TODO
func (l *leaf) unRstrm(ctx context.Context, name string) error {
	return nil
}

func (l leaf) Domain() string {
	inf, err := l.js.AccountInfo(ctx)
	if err != nil {
		log.Println("error getting domain from account info:", err)
		return ""
	}
	return inf.Domain
}

func waitForLeafConnect(ctx context.Context, l *leaf) {
	for {
		_, err := l.jr.AccountInfo(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
}

func main() {
	if dflt.EnvString("SHOWINTFS", "0") == "1" {
		showInterfaces()
	}

	lf, err := newLeaf(ctx, "my", "my", "rasp")
	if err != nil {
		log.Println(err)
		return
	}
	defer lf.nc.Close()
	defer lf.ns.WaitForShutdown() // requires a ctrl-C to terminate
	//defer lf.ns.Shutdown()

	if dflt.EnvString("SHOWSVR", "0") == "1" {
		showServerName(lf)
	}

	lf.hiV1()

	ctx := context.Background()
	createRemoteStream(ctx, lf)
}

func createRemoteStream(ctx context.Context, lf *leaf) {
	mstrm, err := lf.rstrm(ctx, "mstrm", []string{"m.>"})
	if err != nil {
		log.Println(err)
		return
	}

	_ = mstrm
	log.Println("remote stream mstrm created")
}

func embedNATSServer() (*nats.Conn, *server.Server, error) {
	id, dom := nuidAndHash()
	dir := dflt.EnvString("STORE_DIR", "/home/siuyin/embedded_nats")
	passwd := dflt.EnvString("LEAF_PASSWD", "your leaf connection password here")
	host := dflt.EnvString("LEAF_HOST", "rasp.beyondbroadcast.com:8080")
	log.Printf("STORE_DIR=%s LEAF_PASSWD=%s LEAF_HOST=%s", dir, passwd[0:5]+"...", host)
	opts := &server.Options{ServerName: id, JetStream: true, StoreDir: dir, JetStreamDomain: dom, Port: 4222, DontListen: false,
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

func showInterfaces() {
	intf, err := net.Interfaces()
	if err != nil {
		log.Println(err)
		return
	}
	for _, i := range intf {
		fmt.Printf("%s : %x : %#v\n", i.Name, i.HardwareAddr, i)
	}
}

func showServerName(l *leaf) {
	fmt.Printf("Server Node: %s\n", l.ns.Node())
}

func nuidAndHash() (string, string) {
	id := nuid.Next()
	h := fnv.New32a()
	h.Write([]byte(id))
	hash := h.Sum32()
	return id, "D" + strconv.FormatUint(uint64(hash), 36)
}
