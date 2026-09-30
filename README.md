# 🎮 Real-Time Competitive Gaming Intelligence Platform

A high-throughput, distributed streaming platform designed to monitor game server infrastructure, score match quality, profile player behavior, and detect anomalies (aimbots, wallhacks, smurfs) in real time using **Apache Kafka**, **Apache Spark Structured Streaming**, **Go**, and **PySpark**.

---

## 🏗️ Architecture

```text
                         GAME SERVERS / SIMULATOR (Go)
                                      │
                         ┌────────────┴────────────┐
                         │                         │
                   Gameplay events            Server metrics
                         │                         │
                         └────────────┬────────────┘
                                      ▼
                               KAFKA (KRaft)
                                      │
                  ┌───────────────────┼──────────────────┐
                  │                   │                  │
                  ▼                   ▼                  ▼
             gameplay_events     player_events      server_metrics
                  │                   │                  │
                  └───────────────────┼──────────────────┘
                                      ▼
                           SPARK STRUCTURED STREAMING
                                      │
                ┌─────────────┬───────┼─────────┬─────────────┐
                ▼             ▼       ▼         ▼             ▼
             Cheat          Match   Player    Server       Economy
            Detection      Quality  Behavior   Health       Analytics
                │             │       │         │             │
                └─────────────┴───────┼─────────┴─────────────┘
                                      ▼
                               Real-Time State
                                      │
                            ┌─────────┴─────────┐
                            ▼                   ▼
                        FastAPI + WS       Alert Engine (Go)
                            │                   │
                            ▼                   ▼
                      Datastar Dashboard   Postgres / Alerts
                                                
                                      +
                                      
                               Parquet / HDFS
                                      │
                                      ▼
                             Historical Batch ML
```

---

## 🛠️ Technology Stack

| Layer | Technology |
|---|---|
| **Event Simulator** | Go 1.22 (`kafka-go`, concurrent goroutines, rate limiter) |
| **Ingestion** | Apache Kafka 3.7 (KRaft mode - no ZooKeeper) |
| **Stream Processing** | Apache Spark 3.5 (PySpark Structured Streaming, Sliding/Tumbling Windows) |
| **Real-Time Storage** | Redis 7 |
| **Persistent Storage** | PostgreSQL 16 + Parquet on HDFS / Local disk |
| **Alert Engine** | Go 1.22 |
| **Backend API** | Python FastAPI + WebSockets |
| **Frontend** | Datastar v1.0.4 + Jinja2 SSR (SSE patches, zero build step) |

---

## 📁 Project Structure

```text
gaming-intelligence-platform/
├── docker-compose.yml              # Kafka, Spark Master + Workers, Redis, Postgres
├── Makefile                        # Automation shortcuts for building and running
├── README.md                       # Project overview and setup instructions
├── .gitignore                      # Git ignore rules
├── schemas/                        # Avro event contracts
│   ├── gameplay_event.avsc
│   ├── player_event.avsc
│   ├── server_metric.avsc
│   └── alert.avsc
├── simulator/                      # Go Game Event Simulator
│   ├── go.mod
│   ├── cmd/simulator/main.go
│   └── profiles/                   # Statistical player archetypes (YAML)
│       ├── normal_bronze.yaml
│       ├── normal_gold.yaml
│       ├── normal_diamond.yaml
│       ├── cheater_aimbot.yaml
│       ├── cheater_wallhack.yaml
│       ├── smurf.yaml
│       └── toxic.yaml
├── streaming/                      # PySpark Streaming Pipelines
│   ├── requirements.txt
│   └── src/
│       ├── common/
│       │   ├── config.py
│       │   └── schemas.py
│       ├── jobs/                   # Streaming analytics jobs
│       └── ml/                     # ML & Anomaly detection
├── api/                            # FastAPI REST, WebSocket & Dashboard
│   ├── requirements.txt
│   ├── main.py                     # REST + WS endpoints
│   ├── state.py                    # Shared feed / sampler / PG pool
│   ├── dashboard.py                # SSR views + Datastar SSE streams
│   ├── templates/                  # Jinja2 (7 tabs + drill-downs)
│   └── static/                     # dashboard.css
├── benchmarks/                     # Phase 6: tiered load benchmark + collector
│   ├── run_benchmark.sh
│   ├── collect.py
│   └── plot_results.py
└── docs/
    └── ROADMAP.md                  # Comprehensive implementation roadmap
```

---

## 🚦 Roadmap Progress

- [x] **Phase 0: Foundation & Environment Setup** (Docker Compose, schemas, behavior profiles, boilerplate)
- [x] **Phase 1: Game Event Simulator** (Go-based concurrent event generation engine with goroutines & Kafka producer)
- [x] **Phase 2: Core Streaming Pipeline** (Server Health, Cheat Detection & Match Quality jobs with PySpark, Redis sink + Parquet archive, 13 unit tests)
- [x] **Phase 3: Advanced Analytics & Smurf Detection** (Smurf Detection with 100% live precision, CUSUM behavior-shift detection with cross-job boost into cheat scoring, Economy analytics, offline-trained Isolation Forest inference, 31 unit tests)
- [x] **Phase 4: API & Alert Layer** (Go alert engine consuming Kafka alerts with dedup/rate-limiting + PostgreSQL history, 12 REST endpoints incl. match/economy/player drill-downs, WebSocket event & alert feeds, throughput stats)
- [x] **Phase 5: Dashboard & Visualization** (Datastar + Jinja2 server-rendered dashboard: 7 tabs, live event stream, quality histogram, player/match drill-downs, filterable alert history — all served from the API at `:8000`, zero build step)
- [x] **Phase 6: Benchmarking & Historical Analysis** (Automated 5-tier benchmark 1k→20k ev/s with throughput/lag/resource collection → `docs/benchmarks.md` + graph; 6 PySpark batch analyses — skill progression, weapon meta, cheat precision/recall vs archetype labels, quality histogram, server reliability, peak hours — over the Parquet archive)
- [ ] **Phase 7: Polish & Documentation** (Final reporting, BTP submission assets)

---

## 🚀 Quick Start

### 1. Launch the Infrastructure
```bash
make up
```

This starts:
- Kafka Broker on port `9092` (internal Docker) and `9094` (external host)
- Spark Master Web UI at [http://localhost:8080](http://localhost:8080)
- 2 Spark Workers (2 cores, 2GB memory each) with Python ML packages installed
- Redis on port `6379`
- PostgreSQL on host port `5433` (container port `5432`)
  > Containers reach Postgres at `postgres:5432` on the compose network — the
  > engine's and API's `POSTGRES_PORT=5432` defaults assume that. When running
  > `alert-engine` or `api` directly on the **host**, export `POSTGRES_PORT=5433`
  > (the published port), or the connection is refused.

### 2. Verify Kafka Topics
```bash
make kafka-topics
```

### 3. Run Event Simulator
Run against local Kafka:
```bash
make simulator-run
```
Or test locally without Kafka running (dry-run mode):
```bash
make simulator-dry-run
```

### 4. Submit PySpark Streaming Jobs
Submit the Server Health Monitor:
```bash
make spark-submit JOB=server_health
```
Submit the Cheat Detection Pipeline:
```bash
make spark-submit JOB=cheat_detection
```

### 5. Launch FastAPI Backend
```bash
make api-run
```
Interactive Swagger docs available at [http://localhost:8000/docs](http://localhost:8000/docs).

The live dashboard is served from the same server at [http://localhost:8000/](http://localhost:8000/) —
server-rendered HTML with Datastar SSE patches (no build step, no separate frontend container).

---

## 📄 License
MIT License
