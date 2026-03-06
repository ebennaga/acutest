FROM python:3.12-slim

# System deps for psycopg2 + dlt
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt && \
    sed -i 's/import pkg_resources/import importlib.metadata as pkg_resources/' \
        /usr/local/lib/python3.12/site-packages/dlt/helpers/dbt/__init__.py

COPY main.py models.py pipeline.py ./

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
