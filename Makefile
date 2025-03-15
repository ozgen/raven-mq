build:
	@go build -o bin/app cmd/main.go

.PHONY: test run start-mq stop-mq

# Start Raven-MQ in the background
start-mq:
	@echo "Starting Raven-MQ..."
	@./bin/app & echo $$! > ravenmq.pid
	@sleep 3  # Give it time to initialize

# Stop Raven-MQ after tests
stop-mq:
	@echo "Stopping Raven-MQ..."
	@kill $$(cat ravenmq.pid) || true
	@rm -f ravenmq.pid

# Run tests with a running Raven-MQ instance
test: build start-mq
	@echo "Running tests..."
	@go test -v ./...
	@$(MAKE) stop-mq

run: build
	@./bin/app

debug:
	@dlv debug --headless --listen=:2345 --log --api-version=2 ./cmd/main.go
