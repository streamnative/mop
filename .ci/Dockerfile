ARG PULSAR_FQ_IMAGE
FROM ${PULSAR_FQ_IMAGE}

USER root
RUN rm -rf /pulsar/protocols/pulsar-protocol-handler-mqtt-*.nar

COPY ../mqtt-impl/target/pulsar-protocol-handler-mqtt-${PULSAR_FQ_IMAGE}-.nar /pulsar/protocols