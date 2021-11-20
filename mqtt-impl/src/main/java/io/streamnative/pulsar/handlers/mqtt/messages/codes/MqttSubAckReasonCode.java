/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.messages.codes;

import io.netty.handler.codec.mqtt.MqttQoS;

public enum MqttSubAckReasonCode {
    GRANTED_QOS0(0x0),
    GRANTED_QOS1(0x1),
    GRANTED_QOS2(0x2),
    UNSPECIFIED_ERROR(0x80),
    // mqtt version 5
    IMPLEMENTATION_SPECIFIC_ERROR(0x83),
    NOT_AUTHORIZED(0x87),
    TOPIC_FILTER_INVALID(0x8F),
    PACKET_IDENTIFIER_IN_USE(0x91),
    QUOTA_EXCEEDED(0x97),
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED(0x9E),
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED(0xA1),
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED(0xA2);


    private final int shortValue;

    MqttSubAckReasonCode(int shortValue) {
        this.shortValue = shortValue;
    }

    public int value() {
        return shortValue;
    }

    public static MqttSubAckReasonCode qosGranted(MqttQoS qos) {
        switch (qos) {
            case AT_MOST_ONCE:
                return MqttSubAckReasonCode.GRANTED_QOS0;
            case AT_LEAST_ONCE:
                return MqttSubAckReasonCode.GRANTED_QOS1;
            case EXACTLY_ONCE:
                return MqttSubAckReasonCode.GRANTED_QOS2;
            default:
                return MqttSubAckReasonCode.UNSPECIFIED_ERROR;
        }
    }
}
