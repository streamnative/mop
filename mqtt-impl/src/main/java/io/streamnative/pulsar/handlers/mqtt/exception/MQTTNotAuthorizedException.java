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

public class MQTTNotAuthorizedException extends MQTTServerException {
    @Getter
    private final MqttMessageType condition;
    @Getter
    private final String clientId;
    @Getter
    private final int packetId;

    public MQTTNotAuthorizedException(MqttMessageType condition, int packetId,
                                      String topicName, String userRole, String clientId) {
        super(String.format("No authorization to topic=%s, userRole=%s, CId= %s",
                topicName, userRole, clientId));
        this.condition = condition;
        this.clientId = clientId;
        this.packetId = packetId;
    }
    public MQTTNotAuthorizedException(MqttMessageType condition, int packetId,
                                      String userRole, String clientId) {
        super(String.format("No authorization userRole=%s, CId= %s",
                 userRole, clientId));
        this.condition = condition;
        this.clientId = clientId;
        this.packetId = packetId;
    }
}
