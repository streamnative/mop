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
package io.streamnative.pulsar.handlers.mqtt.messages;

import io.netty.handler.codec.mqtt.MqttProperties;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Mqtt property utils.
 * @see <a>https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.pdf</a>
 */
@Slf4j
public class MqttPropertyUtils {

    /**
     * Get session expire interval.
     * @param properties - mqtt properties
     * @return Integer - expire interval value
     */
    @SuppressWarnings("unchecked")
    public static Optional<Integer> getExpireInterval(MqttProperties properties) {
        MqttProperties.MqttProperty<Integer> property = properties
                .getProperty(MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
        return Optional.ofNullable(property.value());
    }
};
