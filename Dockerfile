# syntax=docker/dockerfile:1
ARG JAR_SOURCE=compiled
FROM maven:3.9-eclipse-temurin-21 AS compiled
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2/repository \
    mvn -B -ntp -DskipTests package && cp target/ads-attribution-*.jar /app.jar

FROM scratch AS prebuilt
COPY target/ads-attribution-*.jar /app.jar

FROM ${JAR_SOURCE} AS artifact
FROM eclipse-temurin:21-jre
RUN useradd --system pipeline
USER pipeline
WORKDIR /app
COPY --from=artifact /app.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
