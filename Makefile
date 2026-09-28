.PHONY: leak test bench race vet

leak:
	go run ./cmd/leakcheck -rounds 1000

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -run '^$$' -bench=. -benchmem ./...

vet:
	go vet ./...
