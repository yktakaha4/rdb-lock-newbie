FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y wget lsb-release gnupg postgresql-client

COPY pyproject.toml poetry.lock ./

RUN pip install poetry
RUN poetry install --no-root --sync
