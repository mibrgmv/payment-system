.PHONY: lint test-events test

lint:
	golangci-lint run

test-events:
	@echo "Running shared /events tests..."
	cd events && go test -v ./...

test: test-events
