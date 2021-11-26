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
package io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3;

import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttReasonCode;

/**
 * Mqtt protocol 3.1.x connection reason code.
 *
 * see : http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.doc
 */
public enum Mqtt3SubReasonCode implements MqttReasonCode {
    MAXIMUM_QOS_0(0x00),
    MAXIMUM_QOS_1(0x01),
    MAXIMUM_QOS_2(0x02),
    FAILURE(0x80);

    private final int code;

    Mqtt3SubReasonCode(int code) {
        this.code = code;
    }

    @Override
    public int value() {
        return code;
    }
}
