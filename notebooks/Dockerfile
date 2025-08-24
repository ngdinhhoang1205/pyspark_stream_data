FROM jupyter/pyspark-notebook:python-3.10

# Pre-install common libs
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Maven
USER root
RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*
USER $NB_UID