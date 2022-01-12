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
package io.streamnative.pulsar.handlers.mqtt.exception.handler;
import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MopExceptionHelper {
    private static final Map<MqttMessageType, MqttExceptionHandler> exceptionHandlers = Maps.newConcurrentMap();

    static {
        exceptionHandlers.put(MqttMessageType.CONNECT, new MqttConnExceptionHandler());
        exceptionHandlers.put(MqttMessageType.DISCONNECT, new MqttDisConnExceptionHandler());
        exceptionHandlers.put(MqttMessageType.PUBLISH, new MqttPubExceptionHandler());
        exceptionHandlers.put(MqttMessageType.UNSUBSCRIBE, new MqttUnsubExceptionHandler());
    }


    public static void handle(MqttMessageType type, int identifier, Channel channel, Throwable ex) {
        MqttExceptionHandler mqttExceptionHandler = exceptionHandlers.get(type);
        if (mqttExceptionHandler == null) {
            log.error("Could not found exception handler {}", type);
            throw new IllegalArgumentException(String.format("Could not found exception handler %s", type));
        }
        int protocolVersion = NettyUtils.getConnection(channel).getProtocolVersion();
        if (!MqttUtils.isSupportedVersion(protocolVersion)) {
            log.error("Wrong protocol version present! the protocol version is {}", protocolVersion);
            throw new IllegalArgumentException(
                    String.format("Wrong protocol version present! the protocol version is %s", type));
        }
        if (protocolVersion == MqttVersion.MQTT_3_1_1.protocolLevel()
                || protocolVersion == MqttVersion.MQTT_3_1.protocolLevel()) {
            mqttExceptionHandler.handleVersion3(identifier, channel, ex);
        } else if (protocolVersion == MqttVersion.MQTT_5.protocolLevel()) {
            mqttExceptionHandler.handleVersion5(identifier, channel, ex);
        }
    }

}
