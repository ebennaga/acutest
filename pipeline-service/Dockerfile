FROM python:3.12-slim

# System deps for psycopg2 + dlt
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

# setuptools must be installed FIRST so pkg_resources is available for dlt
RUN pip install --no-cache-dir setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py models.py pipeline.py ./

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]


COPY main.py models.py pipeline.py ./

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
