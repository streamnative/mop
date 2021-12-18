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

import java.util.Arrays;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract ack handler.
 */
@Slf4j
public enum AckHandlerFactory {

    ACK_HANDLER_V3_1(3, new MqttV3xAckHandler()),

    ACK_HANDLER_V3_1_1(4, new MqttV3xAckHandler()),

    ACK_HANDLER_V5(5, new MqttV5AckHandler());

    @Getter
    private int protocol;

    @Getter
    private AckHandler ackHandler;

    AckHandlerFactory(int protocol, AckHandler ackHandler) {
        this.protocol = protocol;
        this.ackHandler = ackHandler;
    }

    public static AckHandlerFactory of(int protocol) {
        return Arrays.stream(values()).filter(e -> e.protocol == protocol).findFirst()
                .orElseThrow(() -> new UnsupportedOperationException("invalid protocol :" + protocol));
    }
}
