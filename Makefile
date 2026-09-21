.PHONY: build test vet lint demo down smoke

build:
	go build ./...

test:
	go test -race ./...

vet:
	go vet ./...
	gofmt -l .

lint:
	golangci-lint run ./... || echo "(install golangci-lint for full lint)"

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
