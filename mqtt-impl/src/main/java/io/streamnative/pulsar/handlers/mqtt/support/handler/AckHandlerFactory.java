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
package io.streamnative.pulsar.handlers.mqtt.support.handler;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract ack handler.
 */
@Slf4j
public class AckHandlerFactory {

    private static final Set<Integer> PROTOCOLS = Sets.newHashSet(3, 4, 5);

    public static AckHandler newAckHandler(Connection connection) {
        int protocol = connection.getProtocolVersion();
        if (PROTOCOLS.contains(protocol)) {
            switch (protocol) {
                case 3:
                case 4:
                    return new MqttV3xAckHandler(connection);
                case 5:
                    return new MqttV5AckHandler(connection);
                default:
                    throw new UnsupportedOperationException("invalid protocol :" + protocol);
            }
        } else {
            throw new UnsupportedOperationException("invalid protocol :" + protocol);
        }
    }
}
