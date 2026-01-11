package nstor

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/siuyin/dflt"
)

// Connect creates a nats connection. Remember to Close the nats connection when you are done with it.
func Connect() (*nats.Conn, error) {
	creds := dflt.EnvString("CREDS", "your nats connection credentials as a string here")
	natsURL := dflt.EnvString("NATS_URL", "wss://connect.ngs.global:443")
	log.Printf("nstor: CREDS[0:10]=%s: NATS_URL=%s", creds[0:10], natsURL)

	nc, err := nats.Connect(natsURL, nats.UserCredentialBytes([]byte(creds)))
	if err != nil {
		return nil, fmt.Errorf("nstor: could not connect to %s with creds[0:10]=%s: %v", natsURL, creds[0:10], err)
	}

	return nc, nil
}

// CreateKeyValue  creates a KV store with id which can hold a maximum of size bytes.
func CreateKeyValue(ctx context.Context, nc *nats.Conn, id string, size int64) (jetstream.KeyValue, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("nstor create JetStream: %v", err)
	}
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: id, MaxBytes: size})
	if err != nil {
		return nil, fmt.Errorf("nstor create KeyValue: %s, size: %d bytes: %v", id, size, err)
	}
	return kv, nil
}
