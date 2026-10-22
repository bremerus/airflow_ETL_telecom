FROM apache/airflow:2.10.2
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.2" -r /requirements.txt