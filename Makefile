.PHONY: build test vet lint demo demo-host down smoke

build:
	go build ./...

test:
	go test -count=1 -race ./...

vet:
	go vet ./...
	test -z "$$(gofmt -l .)"

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { echo "error: golangci-lint not installed (see https://golangci-lint.run/docs/welcome/install/)"; exit 1; }
	golangci-lint run ./...

demo:
	docker compose up -d --build
	@echo "LB on :9000, admin :8080. Try:"
	@echo "  python3 scripts/demo.py"
	@echo "  docker stop tcp-load-balancer-server-b-1"

demo-host:
	docker compose up -d --build server-a server-b server-c
	go run ./cmd/lb --config config.yaml

down:
	docker compose down

smoke:
	python3 scripts/demo.py
