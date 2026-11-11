FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace/java
COPY java_server_client/pom.xml ./pom.xml
COPY java_server_client/src ./src
COPY contracts /workspace/contracts
RUN mvn -B -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/java/target/java-chat.jar /app/java-chat.jar
RUN mkdir -p /data
ENTRYPOINT ["java", "-cp", "/app/java-chat.jar"]
