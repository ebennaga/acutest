# Customer REST API

A Flask REST API that serves customer data from a JSON file.

## Project Structure

```
flask-api/
├── app.py               # Flask application
├── requirements.txt     # Python dependencies
├── Dockerfile           # Multi-stage Docker build
├── .dockerignore
└── data/
    └── customers.json   # 25 sample customers
```

## Endpoints

| Method | Endpoint                  | Description                        |
|--------|---------------------------|------------------------------------|
| GET    | `/api/health`             | Health check                       |
| GET    | `/api/customers`          | Paginated customer list            |
| GET    | `/api/customers/{id}`     | Single customer by `customer_id`   |

### Query Parameters — `GET /api/customers`

| Param   | Type | Default | Description              |
|---------|------|---------|--------------------------|
| `page`  | int  | `1`     | Page number (1-based)    |
| `limit` | int  | `10`    | Records per page (max 100) |

## Response Format

### List
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
  "data": { ... }
}
```

### Error
```json
{
  "error": "Customer not found",
  "customer_id": "CUST-999"
}
```

---

## Running Locally

```bash
pip install -r requirements.txt
python app.py
```

API is available at `http://localhost:5000`.

---

## Running with Docker

### Build
```bash
docker build -t customer-api .
```

### Run
```bash
docker run -p 5000:5000 customer-api
```

---

## Example Requests

```bash
# Health check
curl http://localhost:5000/api/health

# First page, 5 records
curl "http://localhost:5000/api/customers?page=1&limit=5"

# Second page
curl "http://localhost:5000/api/customers?page=2&limit=5"

# Single customer
curl http://localhost:5000/api/customers/CUST-001

# 404 example
curl http://localhost:5000/api/customers/CUST-999
```
