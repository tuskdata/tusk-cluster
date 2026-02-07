# tusk-cluster

Distributed query execution plugin for [TuskData](https://github.com/jearalern/tuskdata) using Apache DataFusion and Arrow Flight.

## Features

- **Distributed SQL Queries**: Execute queries across multiple workers using DataFusion
- **Arrow Flight Protocol**: Efficient data transfer between scheduler and workers
- **Auto File Registration**: Automatically register Parquet, CSV, and JSON files
- **Pipeline Support**: Convert ETL pipelines to distributed SQL
- **Web UI Integration**: Monitor cluster status, workers, and jobs from Tusk Studio
- **Fault Tolerance**: Job persistence with SQLite, automatic retries

## Installation

```bash
# As part of TuskData
pip install tuskdata[cluster]

# Or standalone
pip install tusk-cluster
```

## Quick Start

### Start a Local Cluster

```bash
# Start scheduler + 3 workers (development mode)
tusk cluster-dev --workers 3
```

### Start Components Separately

```bash
# Terminal 1: Start scheduler
tusk cluster-scheduler --host 0.0.0.0 --port 8814

# Terminal 2+: Start workers
tusk cluster-worker --scheduler localhost:8814 --port 8815
tusk cluster-worker --scheduler localhost:8814 --port 8816
```

### Web UI

Once installed, a **Cluster** tab appears in Tusk Studio:

- Connect to remote schedulers
- Start/stop local clusters
- Monitor worker status (CPU, memory)
- Submit and track distributed queries
- View job history and results

## Architecture

```
┌─────────────┐     Arrow Flight      ┌─────────────┐
│   Client    │ ◄──────────────────► │  Scheduler  │
│ (Tusk Web)  │                       │  :8814      │
└─────────────┘                       └──────┬──────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
                    ▼                        ▼                        ▼
             ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
             │   Worker    │          │   Worker    │          │   Worker    │
             │  DataFusion │          │  DataFusion │          │  DataFusion │
             │    :8815    │          │    :8816    │          │    :8817    │
             └─────────────┘          └─────────────┘          └─────────────┘
```

- **Scheduler**: Receives jobs, distributes to workers, aggregates results
- **Workers**: Execute SQL using DataFusion, report status via heartbeat
- **Protocol**: Arrow Flight (gRPC + Arrow IPC) for zero-copy data transfer

## Supported File Formats

Workers can query files directly using DataFusion syntax:

```sql
-- Parquet
SELECT * FROM read_parquet('/path/to/data.parquet')

-- CSV
SELECT * FROM read_csv('/path/to/data.csv')

-- JSON
SELECT * FROM read_json('/path/to/data.json')
```

Files are automatically registered as tables when queries are executed.

## Configuration

### Scheduler Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `8814` | Listen port |

### Worker Options

| Option | Default | Description |
|--------|---------|-------------|
| `--scheduler` | `localhost:8814` | Scheduler address |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `8815` | Listen port |
| `--tusk-url` | `http://localhost:8080` | Tusk Studio URL (for catalog) |

## Requirements

- Python 3.11+
- `tuskdata >= 0.2.0`
- `datafusion >= 51.0.0`
- `pyarrow >= 17.0.0`

## License

MIT License - Jearel Alcantara
