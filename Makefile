.PHONY: all build clean test lint rpm srpm distclean

BINARY_NAME=redborder-monitor
VERSION?= $(shell git describe --abbrev=6 --tags HEAD --always 2>/dev/null || echo "dev")

all: build

build:
	go build -ldflags "-X main.Version=$(VERSION)" -o $(BINARY_NAME) ./cmd/redborder-monitor

clean:
	rm -f $(BINARY_NAME)
	$(MAKE) -C packaging/rpm clean

test:
	go test -v ./...

lint:
	go vet ./...
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	elif [ -f $$(go env GOPATH)/bin/golangci-lint ]; then \
		$$(go env GOPATH)/bin/golangci-lint run ./...; \
	else \
		echo "golangci-lint is not installed. Run 'go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest'"; \
	fi

rpm: clean
	$(MAKE) -C packaging/rpm rpm

srpm: clean
	$(MAKE) -C packaging/rpm srpm

distclean: clean
	$(MAKE) -C packaging/rpm distclean
