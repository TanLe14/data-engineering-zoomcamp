FROM apache/airflow:2.6.0-python3.7 as git_install
USER root

RUN apt-get update && \
    apt-get install -y git
    
FROM git_install as git_clone

RUN git clone https://github.com/TanLe14/data-engineering-zoomcamp.git /opt/airflow/git
# WORKDIR /opt/airflow/git

# RUN git config --global --add safe.directory /opt/airflow/git

# RUN git pull
