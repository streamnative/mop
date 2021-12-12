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
package io.streamnative.pulsar.handlers.mqtt.messages.handler;

import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTProtocolVersionNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtocolAckHandlerHelper {
    private static final Map<Integer, ProtocolAckHandler> handlers = Maps.newConcurrentMap();

    static {
        MqttV3xAckHandler mqttV3AckHandler = new MqttV3xAckHandler();
        handlers.put((int) MqttVersion.MQTT_5.protocolLevel(), new MqttV5AckHandler());
        handlers.put((int) MqttVersion.MQTT_3_1.protocolLevel(), mqttV3AckHandler);
        handlers.put((int) MqttVersion.MQTT_3_1_1.protocolLevel(), mqttV3AckHandler);
    }

    public static ProtocolAckHandler getAndCheckByProtocolVersion(Channel channel) {
        int protocolVersion = NettyUtils.getProtocolVersion(channel);
        ProtocolAckHandler protocolAckHandler = handlers.get(protocolVersion);
        if (protocolAckHandler == null) {
            String clientId = NettyUtils.getClientId(channel);
            throw new MQTTProtocolVersionNotSupportException(clientId, protocolVersion);
        }
        return protocolAckHandler;
    }
}
