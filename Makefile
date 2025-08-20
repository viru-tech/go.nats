# suppress output, run `make XXX V=` to be verbose
V := @

.PHONY: lint
lint:
	$(V)golangci-lint run

.PHONY: test
test: GO_TEST_FLAGS += -race
test:
	$(V)go test -mod=vendor $(GO_TEST_FLAGS) --tags=$(GO_TEST_TAGS) ./...

.PHONY: fulltest
fulltest: GO_TEST_TAGS += integration
fulltest: test

.PHONY: bench
bench:
	$(V)go test -run=XXX -mod=vendor $(GO_TEST_FLAGS) --tags=$(GO_TEST_TAGS) -benchmem -bench=. ./...

.PHONY: clean
clean:
	@echo "Clean golangci cache"
	$(V)golangci-lint cache clean
	@echo "Removing $(OUT_DIR)"
	$(V)rm -rf $(OUT_DIR)

.PHONY: vendor
vendor:
	$(V)go mod tidy
	$(V)go mod vendor
	$(V)git add vendor go.mod go.sum

.PHONY: generate
generate:
	$(V)go generate -x ./...
