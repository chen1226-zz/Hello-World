.PHONY: build test race burst clean

build:
	go build ./...

test:
	go test ./... -count=1

race:
	go test ./... -count=1 -race

# Inject a 10x burst for 5s and report heap peak ratio and burst P99.
# Fails (non-zero exit) if peak heap > 2x baseline or P99 >= 200ms.
burst:
	go run ./cmd/burst

clean:
	go clean ./...
