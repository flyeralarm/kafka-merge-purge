FROM openjdk:11-slim AS tester

WORKDIR /home/cuser

ADD ./gradle /home/cuser/gradle
ADD ./gradlew ./build.gradle.kts ./settings.gradle.kts /home/cuser/
RUN ./gradlew --no-daemon build
ADD ./src /home/cuser/src
RUN ./gradlew --no-daemon check

FROM tester AS builder
ARG VERSION="0.0.0"
RUN ./gradlew --no-daemon -PprojVersion="$VERSION" shadowJar

FROM openjdk:11-slim AS distribution
COPY --from=builder /home/cuser/build/libs/kafka-merge-purge.jar /home/cuser/kafka-merge-purge.jar

WORKDIR /home/cuser

ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=100", "-XX:+UseStringDeduplication", "-jar", "kafka-merge-purge.jar"]

