GO ?= go
TOOLCHAIN ?= go1.27.0
GOCMD = GOTOOLCHAIN=$(TOOLCHAIN) $(GO)
VERSION ?= dev

.PHONY: build test race vet fmt check run docker-build

build:
	$(GOCMD) build -trimpath -ldflags "-X main.version=$(VERSION)" -o bin/hronir ./cmd/hronir

test:
	$(GOCMD) test ./...

race:
	$(GOCMD) test -race ./...

vet:
	$(GOCMD) vet ./...

fmt:
	$(GOCMD) fmt ./...

check: fmt vet test
	git diff --check

run:
	$(GOCMD) run ./cmd/hronir run --config examples/hronir.yaml

docker-build:
	docker build --build-arg VERSION=$(VERSION) -t hronir:$(VERSION) .
