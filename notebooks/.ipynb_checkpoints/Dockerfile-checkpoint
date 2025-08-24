FROM jupyter/pyspark-notebook:python-3.10

# Pre-install common libs
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
