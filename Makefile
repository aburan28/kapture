.PHONY: generate generate-proto generate-deepcopy generate-crds build test docker-build lint

CONTROLLER_GEN = go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5

generate: generate-proto generate-deepcopy generate-crds

generate-proto:
	buf lint
	buf generate

generate-deepcopy:
	$(CONTROLLER_GEN) object paths="./api/v1alpha1/..."

generate-crds:
	$(CONTROLLER_GEN) crd paths="./api/v1alpha1/..." output:crd:artifacts:config=config/crd/bases
	cp config/crd/bases/*.yaml charts/kapture/crds/

build:
	go build ./...

test:
	go test ./...

docker-build:
	docker build -f Dockerfile.hub -t kapture/hub:dev .
	docker build -f Dockerfile.spoke -t kapture/spoke:dev .
	docker build -f Dockerfile.agent -t kapture/agent:dev .
	@if [ -f Dockerfile.replay ]; then \
		docker build -f Dockerfile.replay -t kapture/replay-engine:dev .; \
	else \
		echo "Skipping replay-engine image build: Dockerfile.replay not found"; \
	fi

lint:
	@echo "TODO: run golangci-lint once packages are implemented"