package io.streamnative.pulsar.handlers.mqtt.event;

public class DisableEventCenter implements PulsarEventCenter {
    @Override
    public void register(PulsarEventListener listener) {
        throw new UnsupportedOperationException("Unsupported operation.");
    }

    @Override
    public void unRegister(PulsarEventListener listener) {
        throw new UnsupportedOperationException("Unsupported operation.");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("Unsupported operation.");
    }
}
