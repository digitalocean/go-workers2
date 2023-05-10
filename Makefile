# Redis - used for local development
.PHONY: start-docker
start-docker:
	docker run -p 6379:6379 --name test-redis -d redis

.PHONY: clean-docker
clean-docker:
	docker stop test-redis
	docker rm test-redis

# Test
.PHONY: test
test:
	go test -timeout 120s -v

.PHONY: local-test
local-test: start-docker test clean-docker

# Vendored dependencies
.PHONY: vendor
vendor:
	go mod tidy
	go mod vendor

.PHONY: vendor-upgrade
vendor-upgrade:
	go get -u -d all
	go mod vendor
	go mod tidy

# cmd
.PHONY: build-cmd
build-cmd:
	go build -o ./target/gw2ctl github.com/digitalocean/go-workers2/cmd/gwctl