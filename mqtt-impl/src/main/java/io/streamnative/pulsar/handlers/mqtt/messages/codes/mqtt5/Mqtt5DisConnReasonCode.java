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

/**
 * Mqtt protocol 5.0 disconnect reason code.
 *
 * see : https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.pdf
 */
public enum Mqtt5DisConnReasonCode implements MqttReasonCode {
    NORMAL(0x0),
    WITH_WILL_MESSAGE(0x04),
    UNSPECIFIED_ERROR(0x80),
    MALFORMED_PACKET(0x81),
    PROTOCOL_ERROR(0x82),
    IMPLEMENTATION_SPECIFIC_ERROR(0x83),
    NOT_AUTHORIZED(0x87),
    SERVER_BUSY(0x89),
    SERVER_SHUTTING_DOWN(0x8B),
    KEEP_ALIVE_TIMEOUT(0x8D),
    SESSION_TAKEN_OVER(0x8E),
    TOPIC_FILTER_INVALID(0x8F),
    TOPIC_NAME_INVALID(0x90),
    RECEIVE_MAXIMUM_EXCEEDED(0x93),
    TOPIC_ALIAS_INVALID(0x94),
    PACKET_TOO_LARGE(0x95),
    MESSAGE_RATE_TOO_HIGH(0x96),
    QUOTA_EXCEEDED(0x97),
    ADMINISTRATIVE_ACTION(0x98),
    PAYLOAD_FORMAT_INVALID(0x99),
    RETAIN_NOT_SUPPORTED(0x9A),
    QOS_NOT_SUPPORTED(0x9B),
    USE_ANOTHER_SERVER(0x9C),
    SERVER_MOVED(0x9D),
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED(0x9E),
    CONNECTION_RATE_EXCEEDED(0x9F),
    MAXIMUM_CONNECT_TIME(0xA0),
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED(0xA1),
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED(0xA2);

    private final int code;

    Mqtt5DisConnReasonCode(int code) {
        this.code = code;
    }

    @Override
    public int value() {
        return code;
    }
}
