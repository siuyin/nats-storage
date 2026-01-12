# Leaf node connected to NGS

1. Run  a leaf node:
```
nats-server -c ~/exp/nats-storage/config/leaf.conf
```

2. Run the app:
```
go run ./cmd/03_leaf_to_ngs
```
