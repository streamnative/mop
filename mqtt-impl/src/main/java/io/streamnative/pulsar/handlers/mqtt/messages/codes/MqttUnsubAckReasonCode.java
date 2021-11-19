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

/**
 * Mqtt 5.0 unSubscribe acknowledgement reason code.
 */
public enum MqttUnsubAckReasonCode {
    SUCCESS((short) 0x0),
    NO_SUBSCRIPTION_EXISTED((short) 0x11),
    UNSPECIFIED_ERROR((short) 0x80),
    IMPLEMENTATION_SPECIFIC_ERROR((short) 0x83),
    NOT_AUTHORIZED((short) 0x87),
    TOPIC_FILTER_INVALID((short) 0x8F),
    PACKET_IDENTIFIER_IN_USE((short) 0x91);

    private final short byteValue;

    MqttUnsubAckReasonCode(short byteValue) {
        this.byteValue = byteValue;
    }

    public short value() {
        return byteValue;
    }

}
