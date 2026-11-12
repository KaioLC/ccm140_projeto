FROM maven:3.9.9-eclipse-temurin-17-alpine AS build
WORKDIR /workspace/java
COPY java_server_client/pom.xml ./pom.xml
COPY java_server_client/src ./src
COPY contracts /workspace/contracts
RUN mvn -B -DskipTests package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /workspace/java/target/java-chat.jar /app/java-chat.jar
RUN mkdir -p /data

RUN printf '#!/bin/sh\nexec java -cp /app/java-chat.jar chat.Server "$@"\n' > /app/server && chmod +x /app/server && \
    printf '#!/bin/sh\nexec java -cp /app/java-chat.jar chat.Client "$@"\n' > /app/client && chmod +x /app/client
