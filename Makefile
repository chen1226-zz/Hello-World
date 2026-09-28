.PHONY: all test interop fuzz vet

all: vet test

test:
	go test ./...

# Interop matrix: v1<->v2 bidirectional encode/decode with field
# equality assertions, plus golden-sample regression of the v1 layout.
interop:
	go test ./wire/ -run 'TestInteropMatrix|TestGoldenV1' -v -count=1

# 100k random inputs must not panic, plus a short go-fuzz session.
fuzz:
	go test ./wire/ -run 'TestRandomBytesNoPanic' -count=1
	go test ./wire/ -fuzz=FuzzDecode -fuzztime=15s

vet:
	go vet ./...
	gofmt -l .
