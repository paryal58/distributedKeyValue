FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/app
WORKDIR /app
COPY distributed_key_value_store ./distributed_key_value_store
COPY config ./config
# No third-party dependencies: the node is pure standard library.
VOLUME /data
ENTRYPOINT ["python", "-m", "distributed_key_value_store.server.main"]
