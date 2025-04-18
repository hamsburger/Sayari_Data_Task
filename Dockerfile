FROM python:3.10-slim

# Install system packages required for graphviz
RUN apt-get update && apt-get install -y \
    graphviz \
    graphviz-dev \
    pkg-config \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Optional: Install Graphviz Python packages
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Possible to override CMD when running command line
CMD ["scrapy", "crawl", "business_spider"]