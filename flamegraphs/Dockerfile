FROM debian:buster-slim

WORKDIR /home/work

RUN mkdir -p /usr/share/man/man1 /usr/share/man/man2

RUN apt update
RUN apt -y install --no-install-recommends maven default-jdk perl


COPY ./flame-graph-jfr /home/work/flame-jfr
RUN mvn -f flame-jfr/pom.xml package

COPY ./FlameGraph /home/work/flame-graph

COPY ./recording.jfr /home/work/
COPY ./entrypoint.sh /home/work/

ENV NAME=recording

ENTRYPOINT ./entrypoint.sh