package main

import (
	"errors"
	"log"
	"net/url"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/siuyin/dflt"
)

func main() {
	nc, ns, err := embedNATSServer()
	if err != nil {
		log.Fatal(err)
	}
	defer ns.WaitForShutdown()
	defer nc.Close()
	log.Println("server started")
}

func embedNATSServer() (*nats.Conn, *server.Server, error) {
	dir := dflt.EnvString("STORE_DIR", "/home/siuyin/embedded_nats")
	log.Printf("STORE_DIR=%s", dir)
	opts := &server.Options{JetStream: true, StoreDir: dir, JetStreamDomain: "leaf1", Port: 4223, DontListen: false,
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
