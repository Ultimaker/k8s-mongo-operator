# Install pytest python library as well as add all files in current directory
FROM python:alpine AS base
WORKDIR /usr/src/app
RUN apk add --no-cache git
RUN pip install --upgrade pip==9.0.*
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# This is the container build that will run the "unit tests"
FROM base AS tests
WORKDIR /usr/src/app
COPY requirements-testing.txt ./
RUN pip install -r requirements-testing.txt
ARG cache=1
ADD . .
RUN ENV_NAME=testing ASYNC_TEST_TIMEOUT=15 coverage run --source="mongoOperator" -m pytest
RUN coverage report --skip-covered --show-missing

# This is the container build statements that will create the container meant for deployment
FROM base AS build
WORKDIR /usr/src/app
ADD . .
ENTRYPOINT ["python", "./main.py"]
