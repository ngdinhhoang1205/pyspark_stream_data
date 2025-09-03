FROM apache/airflow:3.0.5-python3.12
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt