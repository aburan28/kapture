.PHONY: generate generate-proto build test docker-build lint

generate: generate-proto
	@echo "TODO: run controller-gen for deepcopy generation and CRD manifests"

generate-proto:
	buf lint
	buf generate

build:
	go build ./...

test:
	go test ./...

docker-build:
	docker build -f Dockerfile.hub -t kapture/hub:dev .
	docker build -f Dockerfile.agents -t kapture/agents:dev .
	docker build -f Dockerfile.agent -t kapture/agent:dev .

lint:
	@echo "TODO: run golangci-lint once packages are implemented"
