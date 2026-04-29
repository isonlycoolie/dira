# DIRA

DIRA is an urban predictive traffic intelligence system for Dar es Salaam. The phase-1 goal is to build the foundation that moves traffic and incident data from source ingestion through Kafka and Spark into PostgreSQL and GCS, then exposes the current state through a read API.

## What This Repository Contains

This repository is a Python monorepo built with Hatch workspace mode.

- `apps/api` - FastAPI service for road segment and congestion reads
- `apps/ingestion` - Source connectors and Kafka publishers
- `apps/pipeline` - Spark streaming jobs and feature processing
- `apps/ml` - Model training and inference services
- `apps/decision_engine` - Routing and control trigger logic
- `libs/schemas` - Shared Pydantic contracts
- `libs/common` - Logging, config, Kafka, storage, and metrics helpers
- `libs/geospatial` - Road network and spatial utilities

## Local Setup

1. Clone the repository and enter the workspace.
2. Create your environment file from the example if needed.
3. Install the workspace tooling and project dependencies.
4. Start the local infrastructure.
5. Run the database migrations.
6. Bootstrap the Dar es Salaam road network.
7. Run the test suite.

```bash
# 1. install dependencies and workspace tooling
make install

# 2. start the local infrastructure
make docker-up

# 3. run database migrations
make migrate

# 4. bootstrap the road network
make bootstrap-roads

# 5. run tests
make test
```

## Architecture

```text
Raw Sources
  Telecom Pings | CCTV Feeds | Fleet GPS | Incidents | Weather APIs
        |
        v
Ingestion Service
  Normalize -> Validate -> Publish to Kafka
        |
        v
Kafka Topics
  dira.raw.*  ->  dira.processed.events  ->  dira.alerts.congestion
        |
        v
Spark Pipeline
  Bronze -> Silver -> Gold transforms
        |
        +------------------------------+
        |                              |
        v                              v
PostgreSQL + PostGIS               GCS Parquet Lake
  road network, traffic events        bronze/silver/gold archives
        |
        v
FastAPI Service
  segment traffic, heatmaps, incidents, predictions
        |
        v
Traffic operators and navigation clients
```

## Development Commands

- `make lint` - run linting
- `make typecheck` - run type checking
- `make test` - run unit tests
- `make test-integration` - run integration tests
- `make docker-up` - start local infrastructure
- `make docker-down` - stop local infrastructure
- `make migrate` - run Alembic migrations
- `make bootstrap-roads` - load the road network

## ADRs

### ADR-001 - Monolith vs Microservices
DIRA is deployed as logical microservices in a monorepo. The services stay separate at the boundary level, but the repository remains unified to keep shared contracts, tests, and infrastructure simple. Kafka handles asynchronous communication, while PostgreSQL handles synchronous reads where the data is already operationally owned.

### ADR-002 - Batch vs Real-time
Telecom and CCTV are treated as real-time inputs because their value drops quickly if delayed. Fleet GPS is treated as hourly batch data, and weather is polled on a short interval. The processing cadence follows the freshness requirement of the source, not a one-size-fits-all streaming assumption.

### ADR-003 - SQL vs NoSQL
PostgreSQL with PostGIS is the system of record for road geometry, traffic events, incidents, and predictions. Redis is used only as a hot cache and deduplication aid. No separate NoSQL datastore is introduced in phase 1 because it would add operational cost without solving a problem the relational store cannot handle.

### ADR-004 - Cloud-managed vs Self-hosted Kafka
Phase 1 uses self-hosted Kafka in Docker Compose so the team can move quickly and keep the stack local and reproducible. The Kafka abstraction layer is intentionally narrow so the broker can be moved to a managed service later without rewriting the rest of the system.
