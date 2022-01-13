package io.streamnative.pulsar.handlers.mqtt.messages.ack;

import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DisconnectAck {
    private final boolean success;
    private final Mqtt5DisConnReasonCode errorReason;
    private final String reasonStr;
}
