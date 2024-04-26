# Builder ############################################
FROM maven:3.8.6-openjdk-11-slim AS builder

WORKDIR /app

RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

COPY pom.xml .
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn clean package


# App ############################################
FROM adoptopenjdk/openjdk11:ubi
WORKDIR /app

COPY --from=builder /app/target/kstreams-playground-jar-with-dependencies.jar app.jar

ENV JAVA_TOOL_OPTIONS "-Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.port=5000 \
  -Dcom.sun.management.jmxremote.rmi.port=5000 \
  -Dcom.sun.management.jmxremote.host=0.0.0.0 "

CMD ["java", "-jar", "app.jar","application.properties"]