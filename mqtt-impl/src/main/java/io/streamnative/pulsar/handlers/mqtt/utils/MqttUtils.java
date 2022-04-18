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
package io.streamnative.pulsar.handlers.mqtt.utils;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.streamnative.pulsar.handlers.mqtt.TopicFilter;

/**
 * Some Mqtt protocol utilities.
 */
public class MqttUtils {
    /**
     * Determine whether we support the mqtt protocol version.
     *
     * @param version - mqtt protocol version
     * @return - Is supported version
     * @see MqttConnectMessage
     */
    public static boolean isSupportedVersion(int version) {
        return version == MqttVersion.MQTT_3_1.protocolLevel()
                || version == MqttVersion.MQTT_3_1_1.protocolLevel()
                || version == MqttVersion.MQTT_5.protocolLevel();
    }

    /**
     * Determine whether the protocol version is mqtt 5.0.
     *
     * @param version -mqtt protocol version
     * @return - Is mqtt 5.0 version.
     */
    public static boolean isMqtt5(int version) {
        return version == MqttVersion.MQTT_5.protocolLevel();
    }

    public static boolean isQosSupported(MqttConnectMessage msg) {
        int willQos = msg.variableHeader().willQos();
        MqttQoS mqttQoS = MqttQoS.valueOf(willQos);
        return mqttQoS == MqttQoS.AT_LEAST_ONCE || mqttQoS == MqttQoS.AT_MOST_ONCE;
    }

    public static boolean isRetainedMessage(MqttPublishMessage msg) {
        return msg.fixedHeader().isRetain();
    }

    public static boolean isRetainedMessage(MqttMessage msg) {
        return msg != null && msg instanceof MqttPublishMessage && msg.fixedHeader().isRetain();
    }

    public static boolean isRegexFilter(String topicFilter) {
        return topicFilter.contains(TopicFilter.SINGLE_LEVEL)
                || topicFilter.contains(TopicFilter.MULTI_LEVEL);
    }
}
