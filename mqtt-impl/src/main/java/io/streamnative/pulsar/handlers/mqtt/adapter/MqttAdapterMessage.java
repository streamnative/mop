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
package io.streamnative.pulsar.handlers.mqtt.adapter;

import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.Getter;
import lombok.Setter;

public class MqttAdapterMessage {

    public static final byte MAGIC = (byte) 0xbabe;
    @Getter
    private final byte version = 0x00;
    @Getter
    private final String clientId;
    @Getter
    private final MqttMessage mqttMessage;
    @Getter
    @Setter
    private boolean isAdapter;

    public MqttAdapterMessage(MqttMessage mqttMessage) {
        this.clientId = "";
        this.mqttMessage = mqttMessage;
    }

    public MqttAdapterMessage(String clientId, MqttMessage mqttMessage) {
        this.clientId = clientId;
        this.mqttMessage = mqttMessage;
    }

    @Override
    public String toString() {
        return "MqttAdapterMessage{"
                + "version=" + version
                + ", clientId='" + clientId
                + ", mqttMessage=" + mqttMessage
                + ", isAdapter=" + isAdapter
                + '}';
    }
}
