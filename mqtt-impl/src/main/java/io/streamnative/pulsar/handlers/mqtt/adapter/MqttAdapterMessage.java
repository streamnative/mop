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
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
@AllArgsConstructor
public class MqttAdapterMessage {

    public static final byte MAGIC = (byte) 0xbabe;
    public static final byte DEFAULT_VERSION = 0x00;

    private final byte version;
    private String clientId;
    private MqttMessage mqttMessage;
    private EncodeType encodeType;

    public MqttAdapterMessage(MqttMessage mqttMessage) {
        this(DEFAULT_VERSION, StringUtils.EMPTY, mqttMessage, EncodeType.ADAPTER_MESSAGE);
    }

    public MqttAdapterMessage(String clientId, MqttMessage mqttMessage) {
        this(DEFAULT_VERSION, clientId, mqttMessage, EncodeType.ADAPTER_MESSAGE);
    }

    public MqttAdapterMessage(String clientId, MqttMessage mqttMessage, boolean fromProxy) {
        this(DEFAULT_VERSION, clientId, mqttMessage, fromProxy ? EncodeType.ADAPTER_MESSAGE : EncodeType.MQTT_MESSAGE);
    }

    public MqttAdapterMessage(byte version, String clientId) {
        this(version, clientId, null, EncodeType.ADAPTER_MESSAGE);
    }

    public boolean fromProxy() {
        return encodeType == EncodeType.ADAPTER_MESSAGE;
    }

    public enum EncodeType {
        MQTT_MESSAGE,
        ADAPTER_MESSAGE
    }
}
