package io.streamnative.pulsar.handlers.mqtt;

public interface MqttAckTracker {

    /**
     * Add out standing packet to a tracker that automatically calls subscription ack by batching.
     *
     * @param outstandingPacket See #{@link OutstandingPacket}
     * @throws NullPointerException when outstandingPacket is null.
     */
    void add(OutstandingPacket outstandingPacket);
}
