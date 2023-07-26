ARG PULSAR_FQ_IMAGE
FROM ${PULSAR_FQ_IMAGE}

ARG MOP_VERSION

USER root
RUN rm -rf /pulsar/protocols/pulsar-protocol-handler-mqtt-*.nar

COPY ../mqtt-impl/target/pulsar-protocol-handler-mqtt-${MOP_VERSION}-.nar /pulsar/protocols