#!/usr/bin/make -f

MAKEFLAGS+=--warn-undefined-variables
SHELL:=/bin/bash
.SHELLFLAGS:=-eu -o pipefail -c
.DEFAULT_GOAL:=help
.SILENT:

# all targets are phony
.PHONY: $(shell egrep -o ^[a-zA-Z_-]+: $(MAKEFILE_LIST) | sed 's/://')

test: ## test
	echo 'Starting $@'
	docker compose run --rm python poetry run python -m unittest discover -v
	echo 'Finished $@'

check: ## check
	echo 'Starting $@'
	docker compose run --rm python poetry run black --check .
	docker compose run --rm python poetry run isort --check .
	echo 'Finished $@'

fix: ## fix
	echo 'Starting $@'
	poetry run black .
	poetry run isort .
	echo 'Finished $@'

mysql: ## mysql
	echo 'Starting $@'
	docker compose exec mysql mysql -u root -pmysql -D mysql
	echo 'Finished $@'

mysql_world: ## mysql_world
	echo 'Starting $@'
	docker compose exec mysql mysql -u root -pmysql -D world
	echo 'Finished $@'

psql: ## psql
	echo 'Starting $@'
	docker compose exec postgres psql -U postgres
	echo 'Finished $@'

help: ## Print this help
	echo 'Usage: make [target]'
	echo ''
	echo 'Targets:'
	awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
