package io.streamnative.pulsar.handlers.mqtt.event;

public interface PulsarEventCenter {

    void register(PulsarEventListener listener);

    void unRegister(PulsarEventListener listener);

    void shutdown();
}
