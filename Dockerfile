FROM python:3.11-slim

WORKDIR /app

# Copy requirements first (faster builds)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your code
COPY . .

# Fly.io needs this to keep container running
CMD ["python", "crypto_warehouse.py"]