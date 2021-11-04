package io.streamnative.pulsar.handlers.mqtt;

import java.util.Arrays;

public class WillMessage {
    private String topic;
    private byte[] message;
    private boolean willFlag;
    public WillMessage() {
    }
    public WillMessage(String topic, byte[] message, boolean willFlag) {
        this.topic = topic;
        this.message = Arrays.copyOf(message, message.length);
        this.willFlag = willFlag;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getMessage() {
        return message;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public boolean isWillFlag() {
        return willFlag;
    }

    public void setWillFlag(boolean willFlag) {
        this.willFlag = willFlag;
    }
}
