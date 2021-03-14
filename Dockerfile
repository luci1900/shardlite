FROM alpine:3.14

RUN wget -O serf.zip https://releases.hashicorp.com/serf/0.8.2/serf_0.8.2_linux_arm.zip
RUN unzip serf.zip
RUN mv serf /usr/local/bin/serf
RUN rm serf.zip

EXPOSE 7373 7946

ENTRYPOINT ["/usr/local/bin/serf", "agent", "-discover=cluster"]
