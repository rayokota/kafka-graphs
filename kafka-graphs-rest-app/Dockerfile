FROM confluentinc/cp-base:latest

ARG KAFKA_GRAPHS_VERSION
ARG ARTIFACT_ID

VOLUME /tmp
ARG JAR_FILE=target/kafka-graphs-rest-app-${KAFKA_GRAPHS_VERSION}.jar
COPY ${JAR_FILE} app.jar
CMD ["/usr/bin/java", "-Dfile.encoding=UTF-8", "-Dserver.port=8888", "-jar", "app.jar"]
EXPOSE 8888

