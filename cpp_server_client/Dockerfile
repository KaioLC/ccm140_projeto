FROM alpine:3.21

RUN apk add --no-cache build-base protoc protobuf-dev zeromq-dev cppzmq && mkdir -p /data

WORKDIR /app

COPY contracts/chat.proto .
COPY cpp_server_client/*.cpp .

RUN protoc --cpp_out=. chat.proto && \
    g++ cpp_server.cpp chat.pb.cc $(pkg-config --cflags --libs protobuf libzmq) -o server && \
    g++ cpp_client.cpp chat.pb.cc $(pkg-config --cflags --libs protobuf libzmq) -o client
