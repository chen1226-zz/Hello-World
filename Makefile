GO ?= go

.PHONY: test page-test vet fmt

test:
	$(GO) test ./...

# 并发翻页实验：后台持续写入，客户端游标翻页，重复 1000 次，
# 断言零重复、零遗漏（可用 PAGE_TEST_RUNS 覆盖次数）。
page-test:
	PAGE_TEST_RUNS=$${PAGE_TEST_RUNS:-1000} $(GO) test -race -count=1 -run TestConcurrentPaging .

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .
