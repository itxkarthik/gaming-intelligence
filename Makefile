.PHONY: init-dirs up down restart logs ps clean kafka-topics kafka-console-consumer simulator-build simulator-run simulator-dry-run spark-submit spark-batch spark-compact train-model api-up test-go test-streaming benchmark benchmark-plot help

# ─── Docker Infrastructure ────────────────────────────────────────────────
init-dirs:
	mkdir -p data/checkpoints data/parquet streaming/models
	chmod 777 data data/checkpoints data/parquet streaming/models 2>/dev/null || true
	-chmod -R 777 data streaming/models 2>/dev/null || true

up: init-dirs
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
	cd simulator && ./bin/simulator --events-per-sec 2000 --duration 5m

simulator-dry-run: simulator-build
	cd simulator && ./bin/simulator --dry-run --events-per-sec 1000 --duration 10s

# ─── Spark Jobs ───────────────────────────────────────────────────────────
spark-submit:
	@if [ -z "$(JOB)" ]; then \
		echo "Usage: make spark-submit JOB=server_health (or cheat_detection, match_quality, advanced_analytics)"; \
		exit 1; \
	fi
	# Resource caps: each job gets exactly 1 core / 768MB so all 4 streaming
	# jobs coexist on the 2-worker (4 cores / 4G) cluster — without them the
	# first app claims every core and the rest wait forever.
	docker exec -e JAVA_TOOL_OPTIONS=-Djava.net.preferIPv4Stack=true gaming-spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--conf spark.cores.max=1 \
		--conf spark.executor.cores=1 \
		--conf spark.executor.memory=512m \
		--conf spark.executor.memoryOverhead=256m \
		/opt/spark-apps/src/jobs/$(JOB).py

# ─── ML Model ─────────────────────────────────────────────────────────────
# Only streaming/models/.gitkeep is tracked, so the directory exists in a
# fresh clone but is owned by the host user; the container runs as uid 185
# (the Spark user) and cannot write the .joblib artifact into it. Make it
# world-writable before training — otherwise the train script dies with
# PermissionError.
train-model:
	mkdir -p streaming/models
	chmod 777 streaming/models
	docker exec -w /opt/spark-apps gaming-spark-master python3 src/ml/train_isolation_forest.py

# ─── API Service ──────────────────────────────────────────────────────────
# `make up` already starts the API at :8000; this rebuilds/restarts it alone
# (needed after any change under api/ — the sources are COPY'd, not mounted).
api-up:
	docker compose up -d --build api

# ─── Tests ────────────────────────────────────────────────────────────────
test-go:
	cd simulator && go test ./... -count=1

test-streaming:
	docker exec -w /opt/spark-apps -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip gaming-spark-master python3 -m pytest tests -q -p no:cacheprovider

# ─── Phase 6: Benchmark & Batch Analysis ─────────────────────────────────
# Batch analysis runs in local mode on the master ON PURPOSE: the 4-core
# cluster is fully subscribed by the streaming jobs (spark.cores.max=1 each),
# so a cluster-mode batch job would queue forever.
spark-batch:
	@if [ -z "$(JOB)" ]; then \
		echo "Usage: make spark-batch JOB=all (or skill, weapon, cheat, quality, servers, peak)"; \
		exit 1; \
	fi
	docker exec -w /opt/spark-apps gaming-spark-master /opt/spark/bin/spark-submit \
		--master 'local[1]' \
		/opt/spark-apps/src/batch/historical_analysis.py --job $(JOB)

# Run only after stopping every streaming job; the command swaps each archive
# directory after its compacted copy has been written successfully.
spark-compact:
	@if [ "$(STREAMS_STOPPED)" != "1" ]; then \
		echo "Stop all streaming jobs, then run: make spark-compact STREAMS_STOPPED=1 [JOB=all] [FILES=4]"; \
		exit 1; \
	fi
	docker exec -w /opt/spark-apps gaming-spark-master /opt/spark/bin/spark-submit \
		--master 'local[1]' \
		/opt/spark-apps/src/batch/compact_parquet.py \
		--job $(if $(JOB),$(JOB),all) --files $(if $(FILES),$(FILES),4) --streams-stopped

benchmark:
	bash benchmarks/run_benchmark.sh

# plot_results.py picks the newest benchmarks/results/*/tiers.jsonl itself;
# pass TIERS=<path> to plot an older run.
benchmark-plot:
	uv run --with matplotlib python3 benchmarks/plot_results.py $(TIERS)

# ─── Cleanup ──────────────────────────────────────────────────────────────
# Streaming checkpoints live in ./data/checkpoints (owned by uid 185) and are
# deliberately NOT removed here — deleting them resets every query's offsets.
clean:
	rm -rf simulator/bin/
	find . -type d -name "__pycache__" -exec rm -rf {} +

# ─── Help ─────────────────────────────────────────────────────────────────
help:
	@echo "🎮 Gaming Intelligence Platform - Commands:"
	@echo "  make up                     Start Kafka, Spark, Redis, Postgres, API, alert engine (builds images)"
	@echo "  make init-dirs              Create writable Spark bind-mount directories"
	@echo "  make down                   Stop all containers"
	@echo "  make restart                down + up"
	@echo "  make ps                     Show container status"
	@echo "  make logs                   Tail container logs"
	@echo "  make kafka-topics           List all Kafka topics"
	@echo "  make kafka-console-consumer TOPIC=<name> Read stream in console"
	@echo "  make simulator-build        Compile the Go simulator binary"
	@echo "  make simulator-run          Execute Go simulator against Kafka"
	@echo "  make simulator-dry-run      Execute Go simulator in dry-run mode (no Kafka)"
	@echo "  make spark-submit JOB=<job> Submit a PySpark streaming job (server_health, cheat_detection, match_quality, advanced_analytics)"
	@echo "  make spark-batch JOB=<job>  Historical batch analysis on the Parquet archive (all, skill, weapon, cheat, quality, servers, peak)"
	@echo "  make spark-compact STREAMS_STOPPED=1 [JOB=<archive|all>] [FILES=4] Compact Parquet archives after stopping streaming jobs"
	@echo "  make train-model            Train the IsolationForest artifact into streaming/models/"
	@echo "  make api-up                 Rebuild + restart the FastAPI container (port 8000)"
	@echo "  make test-go                Run Go simulator unit tests"
	@echo "  make test-streaming         Run PySpark pipeline tests in the Spark container"
	@echo "  make benchmark              Tiered load benchmark (RATES=, DURATION=, DRAIN_MAX=)"
	@echo "  make benchmark-plot         Plot the newest benchmark run to docs/benchmarks.png (TIERS=<file> for another)"
	@echo "  make clean                  Remove simulator binary and __pycache__ dirs"
