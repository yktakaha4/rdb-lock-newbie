FROM python:3.12-slim

WORKDIR /app

RUN apt-get update

COPY pyproject.toml poetry.lock ./

RUN pip install poetry==1.8.3
RUN poetry install --no-root --sync
