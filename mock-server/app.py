import json
import os
from datetime import datetime, timezone
from flask import Flask, jsonify, request, abort

app = Flask(__name__)

DATA_FILE = os.path.join(os.path.dirname(__file__), "data", "customers.json")


def load_customers():
    """Load customer data from JSON file."""
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def find_customer(customer_id: str):
    """Return a single customer by ID or None."""
    customers = load_customers()
    for customer in customers:
        if customer["customer_id"] == customer_id:
            return customer
    return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/api/health", methods=["GET"])
def health_check():
    """Health-check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "customer-api",
        "version": "1.0.0"
    }), 200


@app.route("/api/customers", methods=["GET"])
def get_customers():
    """Return a paginated list of customers.

    Query params:
        page  (int, default 1)   – 1-based page number
        limit (int, default 10)  – records per page (max 100)
    """
    try:
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 10))
    except ValueError:
        return jsonify({"error": "page and limit must be integers"}), 400

    if page < 1:
        return jsonify({"error": "page must be >= 1"}), 400
    if limit < 1 or limit > 100:
        return jsonify({"error": "limit must be between 1 and 100"}), 400

    customers = load_customers()
    total = len(customers)

    start = (page - 1) * limit
    end = start + limit
    page_data = customers[start:end]

    return jsonify({
        "data": page_data,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": (total + limit - 1) // limit
    }), 200


@app.route("/api/customers/<string:customer_id>", methods=["GET"])
def get_customer(customer_id: str):
    """Return a single customer by customer_id."""
    customer = find_customer(customer_id)
    if customer is None:
        return jsonify({
            "error": "Customer not found",
            "customer_id": customer_id
        }), 404

    return jsonify({"data": customer}), 200


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"error": "Method not allowed"}), 405


@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
