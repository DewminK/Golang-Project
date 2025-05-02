go run ./cmd/server -id node1 -addr :9001 -peers node2=localhost:9002,node3=localhost:9003 &
go run ./cmd/server -id node2 -addr :9002 -peers node1=localhost:9001,node3=localhost:9003 &
go run ./cmd/server -id node3 -addr :9003 -peers node1=localhost:9001,node2=localhost:9002