FROM apache/airflow:2.5.1 
ADD requirements.txt . 
RUN python -m pip install --upgrade pip
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt