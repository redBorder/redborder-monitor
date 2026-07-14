.PHONY: all build clean test rpm srpm distclean

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

rpm: clean
	$(MAKE) -C packaging/rpm rpm

srpm: clean
	$(MAKE) -C packaging/rpm srpm

distclean: clean
	$(MAKE) -C packaging/rpm distclean
