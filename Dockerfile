FROM apache/airflow:3.3.2
COPY requirements-airflow.txt /
RUN pip install --no-cache-dir -r /requirements-airflow.txt && pip check