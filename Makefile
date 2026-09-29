.PHONY: up down restart logs ps clean kafka-topics kafka-console-consumer simulator-build simulator-run simulator-dry-run spark-submit train-model api-up api-run test-go test-streaming help

# ─── Docker Infrastructure ────────────────────────────────────────────────
up:
	docker compose up -d --build

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

ps:
	docker compose ps

# ─── Kafka Utilities ──────────────────────────────────────────────────────
kafka-topics:
	docker exec gaming-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-console-consumer:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Usage: make kafka-console-consumer TOPIC=gameplay_events"; \
		exit 1; \
	fi
	docker exec gaming-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $(TOPIC) --from-beginning

# ─── Simulator (Go) ───────────────────────────────────────────────────────
simulator-build:
	cd simulator && go build -o bin/simulator ./cmd/simulator

simulator-run: simulator-build
	cd simulator && ./bin/simulator --players 500 --events-per-sec 2000 --duration 5m

simulator-dry-run: simulator-build
	cd simulator && ./bin/simulator --dry-run --players 200 --events-per-sec 1000 --duration 10s

# ─── Spark Jobs ───────────────────────────────────────────────────────────
spark-submit:
	@if [ -z "$(JOB)" ]; then \
		echo "Usage: make spark-submit JOB=server_health (or cheat_detection, match_quality, advanced_analytics)"; \
		exit 1; \
	fi
	# JAVA_TOOL_OPTIONS: host has broken IPv6 — without preferring IPv4 the Ivy
	# --packages resolution hangs on dead IPv6 routes and reports "not found".
	# Resource caps: each job gets exactly 1 core / 768MB so all 3 streaming
	# jobs coexist on the 2-worker (4 cores / 4G) cluster — without them the
	# first app claims every core and the rest wait forever.
	docker exec -e JAVA_TOOL_OPTIONS=-Djava.net.preferIPv4Stack=true gaming-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=1 \
		--conf spark.executor.cores=1 \
		--conf spark.executor.memory=512m \
		--conf spark.executor.memoryOverhead=256m \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		/opt/spark-apps/src/jobs/$(JOB).py

# ─── ML Model ─────────────────────────────────────────────────────────────
# streaming/models/ is untracked, so a fresh clone has no directory for the
# container (uid 185, the Spark user) to write the .joblib artifact into.
# Recreate it world-writable here before training — otherwise the train script
# dies with PermissionError on os.makedirs.
train-model:
	mkdir -p streaming/models
	chmod 777 streaming/models
	docker exec -w /opt/spark-apps gaming-spark-master python3 src/ml/train_isolation_forest.py

# ─── API Service ──────────────────────────────────────────────────────────
api-up:
	docker compose up -d --build api

api-run:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# ─── Tests ────────────────────────────────────────────────────────────────
test-go:
	cd simulator && go test ./... -count=1

test-streaming:
	docker exec -w /opt/spark-apps -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip gaming-spark-master python3 -m pytest tests -q -p no:cacheprovider

# ─── Cleanup ──────────────────────────────────────────────────────────────
clean:
	rm -rf /tmp/spark-checkpoints/*
	rm -rf simulator/bin/
	find . -type d -name "__pycache__" -exec rm -rf {} +

# ─── Help ─────────────────────────────────────────────────────────────────
help:
	@echo "🎮 Gaming Intelligence Platform - Commands:"
	@echo "  make up                     Start Kafka, Spark, Redis, Postgres (builds images)"
	@echo "  make down                   Stop all containers"
	@echo "  make logs                   Tail container logs"
	@echo "  make kafka-topics           List all Kafka topics"
	@echo "  make kafka-console-consumer TOPIC=<name> Read stream in console"
	@echo "  make simulator-build        Compile the Go simulator binary"
	@echo "  make simulator-run          Execute Go simulator against Kafka"
	@echo "  make simulator-dry-run      Execute Go simulator in dry-run mode (no Kafka)"
	@echo "  make spark-submit JOB=<job> Submit a PySpark streaming job (server_health, cheat_detection, match_quality, advanced_analytics)"
	@echo "  make train-model            Train the IsolationForest artifact (creates streaming/models/)"
	@echo "  make api-up                 Start FastAPI backend in Docker on port 8000"
	@echo "  make api-run                Start FastAPI backend on host (needs local Python deps)"
	@echo "  make test-go                Run Go simulator unit tests"
	@echo "  make test-streaming         Run PySpark pipeline tests in the Spark container"
	@echo "  make clean                  Clean temp caches and binaries"
