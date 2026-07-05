FROM apache/airflow:2.0.1-python3.8

USER root

RUN python -m pip install --upgrade pip

RUN pip install --no-cache-dir \
    eml-parser==1.14.4 \
    beautifulsoup4 \
    python-dateutil \
    boto3==1.15.18 \
    botocore==1.18.18 \
    lxml \
    html5lib

USER airflow

RUN pip install --no-cache-dir --upgrade typing_extensions beautifulsoup4