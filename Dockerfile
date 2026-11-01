FROM python:3.12-slim
WORKDIR /app
COPY miniredis ./miniredis
EXPOSE 6379
VOLUME ["/data"]
CMD ["python", "-m", "miniredis", "--host", "0.0.0.0", "--port", "6379", "--persistence-file", "/data/miniredis_data.json"]
