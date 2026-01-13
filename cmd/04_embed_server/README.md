# Connecting an embedded NATS server as a leaf node

1. Run a regular NATS server as a leaf node hub:
```
nats-server -c ~/exp/nats-storage/conf/leaf-hub.conf
```

1. Connect to the above NATS sever using token authentication:
```
NATS_URL=a@localhost:4222 nats rtt
```
Note the 'a' above is the very insecure one char token. In production it would be more like fdlkj9034290jlksflj .

1. Run the embedded leafnode:
```
go run ./cmd/04_embed_server
```

The leafnode connection to the regular NATS server (hub) is user/password authenticated with user: a and password: a.


