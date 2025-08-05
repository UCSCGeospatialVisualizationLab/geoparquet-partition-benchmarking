# Parquet Performance Testing Environment
# 4-core Docker container with S3 access and network monitoring

FROM python:3.11-slim

# Install system dependencies for networking and monitoring
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    tcpdump \
    iftop \
    nethogs \
    htop \
    iotop \
    net-tools \
    dnsutils \
    iputils-ping \
    nodejs \
    npm \
    git \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Python requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Node.js dependencies and install
COPY package.json package-lock.json* ./
RUN npm install

# Copy application code
COPY . .

# Create non-root user for security
RUN useradd -m -u 1000 testuser && chown -R testuser:testuser /app
USER testuser

# Environment variables
ENV PYTHONPATH=/app
ENV NODE_ENV=development

# Resource limits will be set via docker-compose
# CPU limit: 4 cores
# Memory limit: 8GB

# Expose ports for web services
EXPOSE 3000 8080 8888

# Default command
CMD ["bash"]