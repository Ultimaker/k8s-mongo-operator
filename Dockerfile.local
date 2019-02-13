# Copyright (c) 2018 Ultimaker B.V.
FROM python:3.6-stretch AS base
WORKDIR /usr/src/app

COPY requirements*.txt ./
# install MongoDB tools
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5 && \
    echo "deb http://repo.mongodb.org/apt/debian stretch/mongodb-org/3.6 main" | tee /etc/apt/sources.list.d/mongodb-org-3.6.list && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y mongodb-org-tools mongodb-org-shell

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements-testing.txt

ARG cache=1
ARG KUBERNETES_SERVICE_HOST="localhost"
ARG KUBERNETES_SERVICE_PORT=8081
RUN mkdir -p /var/run/secrets/kubernetes.io/serviceaccount && \
    echo "unit-test" >> /var/run/secrets/kubernetes.io/serviceaccount/token && \
    echo "unit-test" >> /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
ADD . .

ENTRYPOINT ["python", "./main.py"]
