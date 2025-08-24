# Base on Spark official image (already has Hadoop, Spark, Java)
FROM bitnami/spark:3.5.0

# Install a specific Python version
# (bitnami/spark ships with Python 3.11, you can replace it if needed)
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install extra Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Maven
USER root
RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*
USER $NB_UID

# Spark jobs will live in /opt/spark/jobs
WORKDIR /opt/spark/jobs
