FROM python:3.13.7-alpine3.21

WORKDIR /app

RUN apk add --no-cache protoc protobuf-dev && \
    pip install --no-cache-dir pyzmq protobuf

COPY contracts/chat.proto .
RUN protoc --python_out=. chat.proto

COPY reference/reference.py /app/main.py

CMD ["python", "main.py"]
