package io.streamnative.pulsar.handlers.mqtt.event;

public interface PulsarEventListener {

    boolean matchPath(String path);

    void onNodeCreated(String path);

    void onNodeDeleted(String path);
}
