# Copyright (c) 2018 Ultimaker B.V.
FROM python:alpine AS base
WORKDIR /usr/src/app
RUN apk add --no-cache openssl
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# This is the container build that will run the "unit tests"
FROM base AS tests
WORKDIR /usr/src/app
COPY requirements-testing.txt ./
RUN pip install -r requirements-testing.txt
ARG cache=1
ARG KUBERNETES_SERVICE_HOST="localhost"
ARG KUBERNETES_SERVICE_PORT=8081
RUN mkdir -p /var/run/secrets/kubernetes.io/serviceaccount
RUN echo "unit-test" >> /var/run/secrets/kubernetes.io/serviceaccount/token
RUN echo "unit-test" >> /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
ADD . .
RUN ENV_NAME=testing ASYNC_TEST_TIMEOUT=15 coverage run --source="mongoOperator" -m pytest
RUN coverage report --skip-covered --show-missing  --fail-under=100

# This is the container build statements that will create the container meant for deployment
FROM base AS build
WORKDIR /usr/src/app
ENV PYTHONUNBUFFERED=0
ENTRYPOINT ["python", "./main.py"]
ADD . .
