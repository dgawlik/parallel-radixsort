FROM openjdk:15-slim-buster

RUN apt-get update && apt-get -y install linux-perf

COPY build/libs/radix-1.0-SNAPSHOT.jar /home

CMD ["/bin/bash"]

