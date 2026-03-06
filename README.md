# Test Acumen

A multi-service architecture that ingests Flask customer data into PostgreSQL via a FastAPI + dlt pipeline.

## Architecture

```
┌─────────────────┐     HTTP/paginated      ┌──────────────────────┐
│   mock-server   │ ──────────────────────► │  pipeline-service    │
│   (Flask :5001) │                         │  (FastAPI :8000)     │
└─────────────────┘                         └──────────┬───────────┘
                                                       │ SQLAlchemy upsert
                                                       ▼
                                            ┌──────────────────────┐
                                            │     PostgreSQL        │
                                            │     (:5433)           │
                                            └──────────────────────┘
```

## Project Structure

```
customer-platform/
├── docker-compose.yml
├── README.md
├── mock-server/                  ← Flask REST API (port 5001)
│   ├── Dockerfile
│   ├── app.py
│   ├── requirements.txt
│   └── data/
│       └── customers.json        (25 sample customers)
└── pipeline-service/             ← FastAPI + dlt ingestion (port 8000)
    ├── Dockerfile
    ├── main.py                   FastAPI app + all endpoints
    ├── models.py                 SQLAlchemy ORM model
    ├── pipeline.py               dlt source + upsert logic
    └── requirements.txt
```

---

## Prerequisites

Make sure the following ports are free before running:

| Service          | Host Port | Container Port |
|------------------|-----------|----------------|
| mock-server      | 5001      | 5000           |
| pipeline-service | 8000      | 8000           |
| postgres         | 5433      | 5432           |

### Free up port 5000 (Mac AirPlay)
```bash
sudo kill -9 $(sudo lsof -ti :5000)
```

### Free up port 5432 (local PostgreSQL)
```bash
brew services stop postgresql
# or
sudo kill -9 $(sudo lsof -ti :5432)
```

---

## Getting Started

```bash
# 1. Navigate to the project root
cd /path/to/customer-platform

# 2. Build and start all services
docker compose up --build
```

Services start in this order automatically:
```
postgres (healthy) → mock-server (healthy) → pipeline-service (start)
```

---

## Usage

Open a new terminal after all services are running:

### Step 1 — Check all services are healthy
```bash
curl http://localhost:5001/api/health
curl http://localhost:8000/api/health
```

### Step 2 — Run the ingest pipeline
```bash
curl -X POST http://localhost:8000/api/ingest
```
Expected response:
```json
{"status": "success", "records_processed": 25}
```

### Step 3 — Query data from the database
```bash
# List all customers (paginated)
curl http://localhost:8000/api/customers

# With pagination params
curl "http://localhost:8000/api/customers?page=1&limit=5"

# Single customer by ID
curl http://localhost:8000/api/customers/CUST-001
```

### Swagger UI
```
http://localhost:8000/docs
```

---

## Endpoints

### Flask mock-server — `http://localhost:5001`

| Method | Path                    | Description                          |
|--------|-------------------------|--------------------------------------|
| GET    | `/api/health`           | Health check                         |
| GET    | `/api/customers`        | Paginated list (`?page=1&limit=10`)  |
| GET    | `/api/customers/{id}`   | Single customer by ID                |

### FastAPI pipeline-service — `http://localhost:8000`

| Method | Path                    | Description                          |
|--------|-------------------------|--------------------------------------|
| GET    | `/api/health`           | Health check                         |
| POST   | `/api/ingest`           | Fetch from Flask → upsert to PostgreSQL |
| GET    | `/api/customers`        | Paginated results from database      |
| GET    | `/api/customers/{id}`   | Single customer from database        |
| GET    | `/docs`                 | Swagger UI                           |

---

## Response Format

### List customers
```json
{
  "data": [...],
  "total": 25,
  "page": 1,
  "limit": 10,
  "total_pages": 3
}
```

### Single customer
```json
{
  "data": {
    "customer_id": "CUST-001",
    "first_name": "Alice",
    "last_name": "Johnson",
    "email": "alice.johnson@email.com",
    "phone": "+1-555-101-2001",
    "address": "{\"street\": \"123 Maple St\", \"city\": \"Austin\"}",
    "date_of_birth": "1990-03-14",
    "account_balance": 4821.5,
    "created_at": "2021-01-10T08:23:00+00:00"
  }
}
```

### Ingest
```json
{"status": "success", "records_processed": 25}
```

---

## How the Pipeline Works

1. `POST /api/ingest` is called on the FastAPI service
2. The **dlt source** in `pipeline.py` auto-paginates through `GET /api/customers` on the Flask service in batches of 50
3. Each record is type-coerced:
   - address dict → JSON string (TEXT column)
   - date string → Python `date` object
   - datetime string → Python `datetime` object
   - balance float → `Decimal`
4. All records are bulk-upserted via `INSERT … ON CONFLICT DO UPDATE`
5. Re-running ingest is safe — fully idempotent

---

## Database Schema

```sql
CREATE TABLE customers (
    customer_id     VARCHAR(50)    PRIMARY KEY,
    first_name      VARCHAR(100)   NOT NULL,
    last_name       VARCHAR(100)   NOT NULL,
    email           VARCHAR(255)   NOT NULL,
    phone           VARCHAR(20),
    address         TEXT,
    date_of_birth   DATE,
    account_balance DECIMAL(15,2),
    created_at      TIMESTAMP
);
```

---

## Connect Directly to PostgreSQL

```bash
docker compose exec postgres psql -U postgres -d customer_db
```

```sql
-- View all customers
SELECT customer_id, first_name, last_name, account_balance FROM customers;

-- Count total records
SELECT COUNT(*) FROM customers;

-- Find customers with high balance
SELECT first_name, last_name, account_balance
FROM customers
ORDER BY account_balance DESC
LIMIT 5;
```

---

## Stopping Services

```bash
# Stop but keep database data
docker compose down

# Stop and delete all data (clean slate)
docker compose down -v
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `port already in use :5000` | `sudo kill -9 $(sudo lsof -ti :5000)` |
| `port already in use :5432` | `brew services stop postgresql` |
| `curl: connection refused :8000` | Run `docker compose ps` — check all 3 containers are Up |
| `GET /api/customers` returns empty | Run `POST /api/ingest` first |
| pipeline-service keeps restarting | Run `docker compose logs pipeline-service` to see the error |
