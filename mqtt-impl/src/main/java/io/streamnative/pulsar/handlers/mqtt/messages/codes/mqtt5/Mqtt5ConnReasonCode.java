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
package io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttReasonCode;

/**
 * Mqtt protocol 5.0 connection reason code.
 *
 * see : https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.pdf
 */
public enum Mqtt5ConnReasonCode implements MqttReasonCode {
    SUCCESS(0x00),
    UNSPECIFIED_ERROR(0x80),
    MALFORMED_PACKET(0x81),
    PROTOCOL_ERROR(0x82),
    IMPLEMENTATION_SPECIFIC(0x83),
    UNSUPPORTED_PROTOCOL_VERSION(0x84),
    CLIENT_IDENTIFIER_NOT_VALID(0x85),
    BAD_USERNAME_OR_PASSWORD(0x86),
    NOT_AUTHORIZED(0x87),
    SERVER_UNAVAILABLE(0x88),
    SERVER_BUSY(0x89),
    BANNED(0x8A),
    BAD_AUTHENTICATION_METHOD(0x8C),
    TOPIC_NAME_INVALID(0x90),
    PACKET_TOO_LARGE(0x95),
    QUOTA_EXCEEDED(0x97),
    PAYLOAD_FORMAT_INVALID(0x99),
    RETAIN_NOT_SUPPORTED(0x9A),
    QOS_NOT_SUPPORTED(0x9B),
    USE_ANOTHER_SERVER(0x9C),
    SERVER_MOVED(0x9D),
    CONNECTION_RATE_EXCEEDED(0x9F);

    private final int code;

    Mqtt5ConnReasonCode(int code) {
        this.code = code;
    }

    @Override
    public int value() {
        return code;
    }

    /**
     * Convert this enum to netty MqttConnectReturnCode.
     *
     * @return - MqttConnectReturnCode-
     * @see MqttConnectReturnCode
     */
    public MqttConnectReturnCode toConnectionReasonCode() {
        return MqttConnectReturnCode.valueOf(byteValue());
    }
}
