.PHONY: start-docker clean-docker

start-docker:
	docker run -p 6379:6379 --name test-redis -d redis

clean-docker:
	docker stop test-redis
	docker rm test-redis

.PHONY: test local-test

test:
	go test -timeout 45s -v

local-test: start-docker test clean-docker

.PHONY: vendor

vendor:
	go mod tidy
	go mod vendor
