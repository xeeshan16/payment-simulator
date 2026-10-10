PAYSIM_TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable
export PAYSIM_TEST_DATABASE_URL

.PHONY: test lint run up down

test: ## run all tests against the Postgres in PAYSIM_TEST_DATABASE_URL
	go test -race -count=1 ./...

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...

up: ## start Postgres, Kafka and the simulator
	docker compose up --build

down:
	docker compose down -v
