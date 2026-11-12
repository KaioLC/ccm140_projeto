FROM alpine:3.21

RUN apk add --no-cache python3 py3-pyzmq protoc py3-protobuf sqlite && mkdir -p /data

WORKDIR /app

COPY contracts/chat.proto .
RUN protoc --python_out=. chat.proto

COPY python_server_client/*.py .

RUN printf '#!/bin/sh\nexec python3 /app/python_server.py "$@"\n' > /app/server && chmod +x /app/server && \
    printf '#!/bin/sh\nexec python3 /app/python_client.py "$@"\n' > /app/client && chmod +x /app/client
