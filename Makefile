SHELL := /usr/bin/env bash
APP := aws-stuff

.PHONY: all build test lint run generate tools docker-up docker-down coverage lambda-build

all: lint test build

build:
	go build ./...

lint:
	golangci-lint run ./...

coverage:
	go test ./... -coverprofile=coverage.out -covermode=atomic
	go tool cover -func=coverage.out

# Run a specific command (e.g., make run CMD=cmd/sqs_worker)
run:
	@if [ -z "$(CMD)" ]; then echo "Usage: make run CMD=cmd/sqs_worker"; exit 1; fi
	go run $(CMD)

generate:
	go generate ./...

tools:
	go install github.com/golang/mock/mockgen@v1.6.0
	go install github.com/segmentio/golines@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

docker-up:
	docker compose up -d

docker-down:
	docker compose down -v

.PHONY: test
# RUN_INTEGRATION_TESTS=1 to include -tags=integration
# Example: make test RUN_INTEGRATION_TESTS=1
TEST_TAGS :=
ifdef RUN_INTEGRATION_TESTS
TEST_TAGS := -tags=integration
endif

test:
	go test $(TEST_TAGS) ./... -v -cover

lambda-build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/lambda-sqs ./cmd/lambda/sqs
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/lambda-sns ./cmd/lambda/sns
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/lambda-s3 ./cmd/lambda/s3
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/lambda-cron ./cmd/lambda/cron
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/lambda-streams ./cmd/lambda/streams 