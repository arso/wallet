SHELL := /bin/bash
GRADLE := sh ./gradlew

.DEFAULT_GOAL := help
.PHONY: help build test e2e jars

help: ## List targets
	@grep -hE '^[a-zA-Z0-9%_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-28s %s\n", $$1, $$2}'

build: ## Build and run all tests except e2e (needs Docker)
	$(GRADLE) build

test: ## Run all tests except e2e (needs Docker)
	$(GRADLE) test

e2e: ## Run the end-to-end tests on the full stack in Docker (slow)
	$(GRADLE) :e2e:e2eTest

jars: ## Build runnable app jars, no tests
	$(GRADLE) bootJar :bridge:installDist -x test

include local-env.mk
