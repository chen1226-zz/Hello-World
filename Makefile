.PHONY: sec-test test vet

# Security regression tests: bad certificates (self-signed / expired /
# hostname mismatch) must fail, valid certs and injected custom CAs must pass.
sec-test:
	go test -v -count=1 ./...

test: sec-test

vet:
	go vet ./...
	gofmt -l .
