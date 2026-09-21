.PHONY: up down restart logs ps clean kafka-topics kafka-console-consumer simulator-build simulator-run spark-submit help

# ─── Docker Infrastructure ────────────────────────────────────────────────
up:
	docker compose up -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

ps:
	docker compose ps

# ─── Kafka Utilities ──────────────────────────────────────────────────────
kafka-topics:
	docker exec -it gaming-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-console-consumer:
	@if [ -z "$(TOPIC)" ]; then \
		echo "Usage: make kafka-console-consumer TOPIC=gameplay_events"; \
		exit 1; \
	fi
	docker exec -it gaming-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $(TOPIC) --from-beginning

# ─── Simulator (Go) ───────────────────────────────────────────────────────
simulator-build:
	cd simulator && go build -o bin/simulator ./cmd/simulator

simulator-run: simulator-build
	cd simulator && ./bin/simulator --players 1000 --events-per-sec 5000 --duration 1m

# ─── Spark Jobs ───────────────────────────────────────────────────────────
spark-submit:
	@if [ -z "$(JOB)" ]; then \
		echo "Usage: make spark-submit JOB=server_health"; \
		exit 1; \
	fi
	docker exec -it gaming-spark-master /opt/spark/bin/spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
		/opt/spark-apps/src/jobs/$(JOB).py

# ─── Cleanup ──────────────────────────────────────────────────────────────
clean:
	rm -rf /tmp/spark-checkpoints/*
	rm -rf simulator/bin/
	find . -type d -name "__pycache__" -exec rm -rf {} +

# ─── Help ─────────────────────────────────────────────────────────────────
help:
	@echo "🎮 Gaming Intelligence Platform - Commands:"
	@echo "  make up                     Start Kafka, Spark, Redis, Postgres"
	@echo "  make down                   Stop all containers"
	@echo "  make logs                   Tail container logs"
	@echo "  make kafka-topics           List all Kafka topics"
	@echo "  make kafka-console-consumer TOPIC=<name> Read stream in console"
	@echo "  make simulator-build        Compile the Go simulator binary"
	@echo "  make simulator-run          Execute Go simulator with test args"
	@echo "  make spark-submit JOB=<job> Submit a PySpark streaming job"
	@echo "  make clean                  Clean temp caches and binaries"
