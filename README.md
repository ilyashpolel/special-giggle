# TASK

**Goal:** Completely rewrite the project to use **AWS SDK for Go v2** instead of v1.  
- Create a new branch: `feature/aws-sdk-update`  
- Update all repository layers (`repository/`) and AWS sessions to use SDK v2.  
- Ensure all services (`service/`) work with the new SDK.  
- Update tests (unit and integration) for compatibility with SDK v2.  
- After completion, open a **pull request to `dev`** for review.  


# AWS Events Playground (Go, aws-sdk-go v1)

A teaching project demonstrating event-driven processing on AWS using Go and the official v1 SDK:
- SQS messages (JSON parsing and DynamoDB persistence)
- SNS notifications (log and forward to another SQS queue)
- S3 object-created events (read object and persist to DynamoDB)
- Scheduled task (CloudWatch Events/Cron-style) publishing metrics to CloudWatch
- DynamoDB Streams (replicate updates to a replica table)

The project follows a clean structure (`cmd/`, `internal/`, `pkg/`), uses `viper` for configuration, `zap` for logging, `gomock` for mocks, Docker + LocalStack for local integration tests, and GitHub Actions for CI.

## Structure
- `cmd/`
  - Workers (local CLI): `sqs_worker/`, `sns_worker/`, `s3_worker/`, `timer_worker/`, `streams_worker/`
  - Lambda handlers: `cmd/lambda/{sqs,sns,s3,cron,streams}`
- `internal/`
  - `config/`: viper-based config loader from `.env` and env vars
  - `logger/`: zap logger factory
  - `aws/`: session helper for AWS SDK v1 (with LocalStack support)
  - `repository/`: AWS repositories (DynamoDB, SQS, SNS, S3, CloudWatch, Streams)
  - `service/`: business logic layer (SQS/SNS/S3/cron/streams)
- `pkg/`
  - `models/`: shared data models

## Prerequisites
- Go 1.22+
- Docker (for LocalStack)
- Make

## Setup
1) Install tools
```bash
make tools
```

2) Start LocalStack
```bash
docker compose up -d
```

3) Create `.env` (see keys below)
```
ENVIRONMENT=local
AWS_REGION=us-east-1
LOCALSTACK_ENDPOINT=http://localhost:4566
DYNAMO_TABLE=app-items
DYNAMO_REPLICA_TABLE=app-items-replica
DYNAMO_STREAM_ARN=
SQS_QUEUE_URL=
SQS_FORWARD_QUEUE_URL=
SNS_FORWARD_TOPIC_ARN=
S3_BUCKET=
TIMER_INTERVAL_SECONDS=15
CLOUDWATCH_NAMESPACE=AppMetrics
SHUTDOWN_GRACE_PERIOD_SECONDS=5
```

## Run local workers
From `aws_stuff/` directory:
```bash
make run CMD=cmd/sqs_worker
make run CMD=cmd/sns_worker
make run CMD=cmd/s3_worker
make run CMD=cmd/timer_worker
make run CMD=cmd/streams_worker
```

## Tests
- Unit tests (default):
```bash
make test
```

- Integration tests (require LocalStack):
  - Bash/zsh:
    ```bash
    RUN_INTEGRATION_TESTS=1 make test
    ```
  - PowerShell:
    ```powershell
    $env:RUN_INTEGRATION_TESTS='1'; make test
    ```

Integration test suite provisions test resources on LocalStack and verifies:
- SQS → service → DynamoDB
- SNS → SQS subscription → forward to another queue
- S3 Put → SQS notification → service → DynamoDB
- DynamoDB Streams → service → replica table

Unit test coverage is ≥ 70% (service package is ~86%). CI runs lint, unit tests, and builds.

## Build Lambda handlers
Produce Linux/amd64 binaries under `bin/`:
```bash
make lambda-build
```
This builds:
- `bin/lambda-sqs` (SQS)
- `bin/lambda-sns` (SNS)
- `bin/lambda-s3` (S3)
- `bin/lambda-cron` (scheduled)
- `bin/lambda-streams` (DynamoDB Streams)

Each handler initializes repositories and delegates to the `service` methods. Configure via environment variables (see `.env` keys).

## Configuration
Config is loaded from `.env` and environment variables via `viper`. Useful keys:
- `ENVIRONMENT` (local|prod)
- `AWS_REGION`
- `LOCALSTACK_ENDPOINT` (use `http://localhost:4566` for LocalStack)
- `DYNAMO_TABLE`, `DYNAMO_REPLICA_TABLE`
- `SQS_QUEUE_URL`, `SQS_FORWARD_QUEUE_URL`
- `SNS_FORWARD_TOPIC_ARN`
- `S3_BUCKET`
- `TIMER_INTERVAL_SECONDS`, `CLOUDWATCH_NAMESPACE`
- `SHUTDOWN_GRACE_PERIOD_SECONDS`

## Lint
```bash
make lint
```

## Notes
- Repositories are thin wrappers over the SDK v1 clients.
- `service` encapsulates business logic and is fully mock-tested with `gomock`.
- Integration tests are tagged with `//go:build integration` and are skipped unless `RUN_INTEGRATION_TESTS=1` is set. 