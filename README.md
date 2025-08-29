# Data Transfer Monitoring System

A monitoring system of file transfers from the Summit to the USDF.

## Local Development Prerequisites

- Docker and Docker Compose
- Python 3.11 or higher
- uv (Python package manager)

## Installation

### Installing uv

uv is a fast Python package installer and resolver written in Rust. Choose one of the following installation methods:

#### macOS/Linux
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### Using pip
```bash
pip install uv
```

#### Using Homebrew (macOS)
```bash
brew install uv
```

After installation, verify uv is installed:
```bash
uv --version
```

### First-Time Setup

1. Clone the repository and navigate to the project directory:
```bash
cd data_transfer_monitoring
```

2. Install Python dependencies using uv:
```bash
uv sync
```

This will create a virtual environment and install all dependencies specified in `pyproject.toml`.

## Running the Application

### Starting the Full Stack with Docker Compose

The easiest way to run the entire application stack is using Docker Compose, which will start all required services including:
- DTM application
- Kafka
- PostgreSQL
- LocalStack (AWS S3 emulation)
- Prometheus
- Grafana
- Loki (logging)
- Promtail (log aggregation)

```bash
docker compose up
```

To run in detached mode (background):
```bash
docker compose up -d
```

To stop all services:
```bash
docker compose down
```

To stop and remove all data volumes:
```bash
docker compose down -v
```

### Running Local Producers

To generate test data and send messages to the system, run the local producers script:

```bash
uv run python local_producers.py
```

This script will produce sample messages to the Kafka topics that the monitoring system consumes.

## Service Endpoints

Once running, the following services will be available:

- **DTM Application**: http://localhost:8000
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Kafka UI**: http://localhost:8080
- **LocalStack (S3)**: http://localhost:4566

## Development

### Running Tests

```bash
uv run pytest tests/
```

### Running the Application Locally (without Docker)

If you want to run the DTM application locally for development:

1. Ensure all dependent services are running via Docker Compose:
```bash
docker compose up kafka postgres localstack
```

2. Set required environment variables (see `docker-compose.yml` for full list)

3. Run the application:
```bash
uv run python main.py
```

### Code Formatting and Linting

The project uses Ruff for linting and formatting:

```bash
# Check for linting issues
uv run ruff check .

# Format code
uv run ruff format .
```

## Project Structure

```
data_transfer_monitoring/
├── docker-compose.yml      # Docker Compose configuration
├── Dockerfile             # DTM application container
├── main.py               # Main application entry point
├── local_producers.py    # Test data generator
├── listeners/            # Kafka consumer implementations
├── models/              # Data models and schemas
├── shared/              # Shared utilities and configurations
├── tests/               # Test suite
├── k8s/                 # Kubernetes configurations
├── prometheus.yml       # Prometheus configuration
├── loki-config.yaml     # Loki configuration
└── promtail-config.yaml # Promtail configuration
```

## Environment Variables

Key environment variables (configured in `docker-compose.yml`):

- `POSTGRES_CONNECTION_STRING`: PostgreSQL connection details
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`: AWS credentials (test values for LocalStack)
- `LOCAL_S3_ENDPOINT`: LocalStack S3 endpoint
- `FILE_NOTIFICATION_KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address
- `END_READOUT_KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address for end readout events

## Troubleshooting

### Services not starting
- Check Docker daemon is running: `docker ps`
- Check for port conflicts: Ensure ports 8000, 3000, 9090, 8080, 4566 are available
- View logs: `docker compose logs [service-name]`

### Dependency issues
- Clear uv cache: `uv cache clean`
- Recreate virtual environment: `rm -rf .venv && uv sync`

### Kafka connection issues
- Ensure Kafka is fully started before running producers: `docker compose logs kafka`
- Check topic creation: Topics are auto-created on first use
