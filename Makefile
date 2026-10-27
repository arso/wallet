SHELL := /bin/bash
GRADLE := sh ./gradlew
RUN := .run
APPS := wallet-api bridge mocks completion-service transfer-executor timeout-checker
JAR = $(1)/build/libs/$(1)-0.0.1-SNAPSHOT.jar

.DEFAULT_GOAL := help
.PHONY: help build test jars up-deps down-deps clean-deps up-all down-all start-apps stop-apps restart-apps status logs

help: ## List targets
	@grep -E '^[a-zA-Z%_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

build: ## Build and run all tests (needs Docker)
	$(GRADLE) build

test: ## Run all tests (needs Docker)
	$(GRADLE) test

jars: ## Build runnable app jars, no tests
	$(GRADLE) bootJar :bridge:installDist -x test

up-deps: ## Start DynamoDB Local, Kafka, Jaeger and Prometheus
	docker compose up -d
	@$(call wait_for,DynamoDB Local,http://localhost:8000)

down-deps: ## Stop dependencies (keeps volumes)
	docker compose down

clean-deps: ## Stop dependencies and remove their volumes
	docker compose down -v

up-all: up-deps jars start-apps ## Dependencies, jars and every app in the background

down-all: stop-apps down-deps ## Stop every app and the dependencies

start-apps: ## Start every app in the background (jars must exist), logs in .run/
	@mkdir -p $(RUN)
	@$(call start,wallet-api,java -jar $(call JAR,wallet-api))
	@$(call wait_for,wallet-api,http://localhost:8081/actuator/health/readiness,-f)
	@$(call start,bridge,JAVA_OPTS=--enable-native-access=ALL-UNNAMED bridge/build/install/bridge/bin/bridge)
	@$(foreach app,mocks completion-service transfer-executor timeout-checker,$(call start,$(app),java -jar $(call JAR,$(app)));)

stop-apps: ## Stop every app started by start-apps
	@for f in $(RUN)/*.pid; do \
		[ -f "$$f" ] || continue; \
		kill $$(cat $$f) 2>/dev/null && echo "stopped $$(basename $$f .pid)"; \
		rm -f $$f; \
	done

restart-apps: stop-apps jars start-apps ## Rebuild jars and restart every app

status: ## Show which apps are running
	@for app in $(APPS); do \
		if [ -f $(RUN)/$$app.pid ] && kill -0 $$(cat $(RUN)/$$app.pid) 2>/dev/null; then s=running; else s=stopped; fi; \
		printf "  %-20s %s\n" $$app $$s; \
	done

logs: ## Follow every app log
	tail -f $(RUN)/*.log

logs-%: ## Follow one app log, e.g. make logs-wallet-api
	tail -f $(RUN)/$*.log

run-%: ## Run one app in the foreground with Gradle, e.g. make run-mocks
	$(GRADLE) :$*:$(if $(filter bridge,$*),run,bootRun)

define start
if [ -f $(RUN)/$(1).pid ] && kill -0 $$(cat $(RUN)/$(1).pid) 2>/dev/null; then \
	echo "$(1) already running"; \
else \
	nohup env $(2) > $(RUN)/$(1).log 2>&1 & echo $$! > $(RUN)/$(1).pid; \
	echo "started $(1), log $(RUN)/$(1).log"; \
fi
endef

define wait_for
for i in $$(seq 60); do curl -s $(3) -o /dev/null $(2) && exit 0; sleep 1; done; echo "$(1) not up after 60s"; exit 1
endef
