FROM maven:3.5.2-jdk-8-alpine AS MAVEN_BUILD
COPY pom.xml /build/
COPY src /build/src/
WORKDIR /build/
RUN mvn package

FROM openjdk:8-jre-alpine
WORKDIR /app
COPY --from=MAVEN_BUILD /build/target/assessment-1.0-SNAPSHOT-jar-with-dependencies.jar /app/
COPY sensor_data.csv /app/
COPY producer.properties /app/
COPY topic.properties /app/
ENTRYPOINT ["java", "-jar", "assessment-1.0-SNAPSHOT-jar-with-dependencies.jar"]