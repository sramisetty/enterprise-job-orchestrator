#!/bin/bash
# Setup script for Enterprise Job Orchestrator with Spark and Airflow integration

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
    exit 1
}

info() {
    echo -e "${BLUE}[INFO] $1${NC}"
}

# Check if Docker is installed
check_docker() {
    log "Checking Docker installation..."
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed. Please install Docker first."
    fi

    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed. Please install Docker Compose first."
    fi

    log "Docker and Docker Compose are installed ✓"
}

# Create necessary directories
create_directories() {
    log "Creating necessary directories..."

    directories=(
        "data/raw"
        "data/processed"
        "data/analytics"
        "data/archive"
        "data/security"
        "data/reports"
        "data/quality"
        "logs/airflow"
        "logs/spark"
        "logs/orchestrator"
        "plugins"
        "config/grafana/dashboards"
        "config/grafana/datasources"
        "tmp/checkpoints"
        "tmp/dead_letter_queue"
        "tmp/spark-checkpoints"
        "tmp/spark-events"
    )

    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        log "Created directory: $dir"
    done
}

# Generate environment files
generate_env_files() {
    log "Generating environment files..."

    # Generate .env file
    cat > .env << EOF
# Database Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres

# Airflow Configuration
AIRFLOW_UID=$(id -u)
AIRFLOW_GID=0
AIRFLOW_PROJ_DIR=./

# Spark Configuration
SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379

# Application Configuration
APP_ENV=development
LOG_LEVEL=INFO
EOF

    log "Generated .env file"
}

# Initialize database schemas
init_databases() {
    log "Creating database initialization script..."

    cat > scripts/init-databases.sql << EOF
-- Create databases for different components
CREATE DATABASE airflow;
CREATE DATABASE enterprise_jobs;

-- Create users
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE USER orchestrator WITH PASSWORD 'orchestrator';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE enterprise_jobs TO orchestrator;

-- Grant connection privileges
GRANT CONNECT ON DATABASE airflow TO airflow;
GRANT CONNECT ON DATABASE enterprise_jobs TO orchestrator;
EOF

    log "Created database initialization script"
}

# Create Dockerfile for the orchestrator
create_dockerfile() {
    log "Creating Dockerfile for Enterprise Job Orchestrator..."

    cat > Dockerfile << 'EOF'
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Java for Spark
RUN apt-get update && apt-get install -y openjdk-11-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Install the package
RUN pip install -e .

# Create non-root user
RUN useradd --create-home --shell /bin/bash orchestrator
RUN chown -R orchestrator:orchestrator /app
USER orchestrator

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port
EXPOSE 8000

# Default command
CMD ["python", "-m", "enterprise_job_orchestrator.cli.main", "start", "--host", "0.0.0.0", "--port", "8000"]
EOF

    log "Created Dockerfile"
}

# Create configuration files
create_config_files() {
    log "Creating configuration files..."

    # Prometheus configuration
    mkdir -p config
    cat > config/prometheus.yml << EOF
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  # - "first_rules.yml"
  # - "second_rules.yml"

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'orchestrator'
    static_configs:
      - targets: ['orchestrator-api:8000']
    metrics_path: '/metrics'

  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-webserver:8080']
    metrics_path: '/admin/metrics'

  - job_name: 'spark-master'
    static_configs:
      - targets: ['spark-master:8080']
    metrics_path: '/metrics/prometheus'
EOF

    # Grafana datasources
    mkdir -p config/grafana/datasources
    cat > config/grafana/datasources/prometheus.yml << EOF
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
EOF

    # File browser configuration
    cat > config/filebrowser.json << EOF
{
  "port": 80,
  "baseURL": "",
  "address": "",
  "log": "stdout",
  "database": "/database.db",
  "root": "/srv"
}
EOF

    log "Created configuration files"
}

# Set up Python environment
setup_python_env() {
    log "Setting up Python virtual environment..."

    if [ ! -d "venv" ]; then
        python3 -m venv venv
        log "Created Python virtual environment"
    fi

    source venv/bin/activate
    pip install --upgrade pip

    log "Python environment ready"
}

# Generate sample data and jobs
generate_sample_data() {
    log "Generating sample data and job examples..."

    # Create sample CSV data
    cat > data/raw/sample_transactions.csv << EOF
transaction_id,customer_id,amount,timestamp,category,status
1,CUST001,150.50,2024-01-01T10:00:00Z,retail,completed
2,CUST002,75.25,2024-01-01T10:15:00Z,online,completed
3,CUST003,200.00,2024-01-01T10:30:00Z,retail,completed
4,CUST004,50.00,2024-01-01T10:45:00Z,food,completed
5,CUST005,300.75,2024-01-01T11:00:00Z,retail,completed
EOF

    # Create sample job configuration
    cat > examples/sample_job_config.json << EOF
{
  "job_name": "sample_data_processing",
  "job_type": "data_processing",
  "execution_engine": "spark",
  "job_data": {
    "input_path": "/data/raw/sample_transactions.csv",
    "output_path": "/data/processed/sample_output",
    "processing_logic": {
      "transformations": [
        {
          "type": "filter",
          "condition": "amount > 100"
        },
        {
          "type": "groupby",
          "columns": ["category"],
          "aggregations": {
            "total_amount": "sum(amount)",
            "transaction_count": "count(*)"
          }
        }
      ]
    }
  },
  "priority": "medium",
  "timeout_seconds": 3600
}
EOF

    log "Generated sample data and configurations"
}

# Start services
start_services() {
    log "Starting services with Docker Compose..."

    # Build and start services
    docker-compose up -d --build

    log "Services started. Waiting for health checks..."
    sleep 30

    # Check service health
    check_service_health
}

# Check service health
check_service_health() {
    log "Checking service health..."

    services=(
        "postgres:5432"
        "redis:6379"
        "spark-master:8080"
    )

    for service in "${services[@]}"; do
        host=$(echo $service | cut -d':' -f1)
        port=$(echo $service | cut -d':' -f2)

        if timeout 10 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then
            log "$service is healthy ✓"
        else
            warn "$service is not responding"
        fi
    done
}

# Initialize Airflow
init_airflow() {
    log "Initializing Airflow database..."

    docker-compose exec airflow-webserver airflow db init
    docker-compose exec airflow-webserver airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin

    log "Airflow initialized with admin/admin credentials"
}

# Display service URLs
show_service_urls() {
    log "Enterprise Job Orchestrator setup complete!"
    info ""
    info "🚀 Service URLs:"
    info "  • Orchestrator API: http://localhost:8000"
    info "  • Airflow UI: http://localhost:8081 (admin/admin)"
    info "  • Spark Master UI: http://localhost:8080"
    info "  • Grafana: http://localhost:3000 (admin/admin)"
    info "  • Prometheus: http://localhost:9090"
    info "  • File Browser: http://localhost:8082"
    info ""
    info "📊 Database:"
    info "  • PostgreSQL: localhost:5432"
    info "  • Redis: localhost:6379"
    info ""
    info "🔧 Next steps:"
    info "  1. Check service health: docker-compose ps"
    info "  2. View logs: docker-compose logs -f [service-name]"
    info "  3. Run sample job: python examples/run_sample_job.py"
    info "  4. Access Airflow to see example DAGs"
    info ""
    info "📚 Documentation: ./docs/"
}

# Main setup function
main() {
    log "🚀 Starting Enterprise Job Orchestrator setup..."

    check_docker
    create_directories
    generate_env_files
    init_databases
    create_dockerfile
    create_config_files
    generate_sample_data
    start_services
    init_airflow
    show_service_urls

    log "✅ Setup completed successfully!"
}

# Check if script is being sourced or executed
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi