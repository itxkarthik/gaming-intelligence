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
                      React Dashboard      Postgres / Alerts
                                                
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
| **Frontend** | React + TypeScript + Recharts |

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
├── api/                            # FastAPI REST & WebSocket Backend
│   ├── requirements.txt
│   └── main.py
└── docs/
    └── ROADMAP.md                  # Comprehensive implementation roadmap
```

---

## 🚦 Roadmap Progress

- [x] **Phase 0: Foundation & Environment Setup** (Docker Compose, Avro schemas, behavior profiles, boilerplate)
- [ ] **Phase 1: Game Event Simulator** (Go-based event generation engine with goroutines & Kafka producer)
- [ ] **Phase 2: Core Streaming Pipeline** (Server Health, Cheat Detection, Match Quality with PySpark)
- [ ] **Phase 3: Advanced Analytics & ML** (Smurf detection, CUSUM behavioral shift, Isolation Forest)
- [ ] **Phase 4: Alert Engine & API Layer** (Go alert deduplicator + FastAPI WebSockets)
- [ ] **Phase 5: Dashboard & Visualization** (React UI with live event streams & charts)
- [ ] **Phase 6: Benchmarking & Historical Analysis** (Throughput vs. Latency evaluation)
- [ ] **Phase 7: Polish & Documentation** (Final reporting, BTP submission assets)

---

## 🚀 Quick Start (Phase 0)

### 1. Launch the Infrastructure
```bash
make up
```

This starts:
- Kafka Broker on port `9092` (internal) and `9094` (external)
- Spark Master Web UI at [http://localhost:8080](http://localhost:8080)
- 2 Spark Workers (2 cores, 2GB memory each)
- Redis on port `6379`
- PostgreSQL on port `5432`

### 2. Verify Kafka Topics
```bash
make kafka-topics
```

### 3. Build & Test Simulator CLI
```bash
make simulator-run
```

---

## 📄 License
MIT License
