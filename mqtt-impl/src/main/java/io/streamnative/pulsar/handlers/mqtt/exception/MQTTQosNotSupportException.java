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
package io.streamnative.pulsar.handlers.mqtt.exception;

import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.Getter;

public class MQTTQosNotSupportException extends MQTTServerException {
    @Getter
    private final MqttMessageType condition;

    public MQTTQosNotSupportException(MqttMessageType condition,
                                      String clientId, int qos) {
        super(String.format("The server do not support that qos clientId=%s qos=%s",
                clientId, qos));
        this.condition = condition;
    }
}
