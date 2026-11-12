FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler sqlite3 && rm -rf /var/lib/apt/lists/* && mkdir -p /data

WORKDIR /app

COPY python_server_client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY contracts/chat.proto .
RUN protoc --python_out=. chat.proto

COPY python_server_client/*.py .

RUN printf '#!/bin/sh\nexec python3 /app/python_server.py "$@"\n' > /app/server && chmod +x /app/server && \
    printf '#!/bin/sh\nexec python3 /app/python_client.py "$@"\n' > /app/client && chmod +x /app/client
