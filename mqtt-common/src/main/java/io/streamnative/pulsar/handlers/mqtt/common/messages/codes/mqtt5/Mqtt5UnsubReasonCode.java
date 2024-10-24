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

package io.streamnative.pulsar.handlers.mqtt.common.messages.codes.mqtt5;

import io.streamnative.pulsar.handlers.mqtt.common.messages.codes.MqttReasonCode;

/**
 * Mqtt protocol 5.0 unsubscription acknowledgement reason code.
 *
 * see : https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.pdf
 */
public enum Mqtt5UnsubReasonCode implements MqttReasonCode {
    SUCCESS(0x0),
    NO_SUBSCRIPTION_EXISTED(0x11),
    UNSPECIFIED_ERROR(0x80),
    IMPLEMENTATION_SPECIFIC_ERROR(0x83),
    NOT_AUTHORIZED(0x87),
    TOPIC_FILTER_INVALID(0x8F),
    PACKET_IDENTIFIER_IN_USE(0x91);

    private final int code;

    Mqtt5UnsubReasonCode(int code) {
        this.code = code;
    }

    public int value() {
        return code;
    }

}
