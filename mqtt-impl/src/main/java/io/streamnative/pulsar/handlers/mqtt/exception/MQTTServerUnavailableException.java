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

import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.CONNECT_ACK;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.DISCONNECTED;
import io.netty.handler.codec.mqtt.MqttMessageType;

public class MQTTServerUnavailableException extends MQTTServerException {

    public MQTTServerUnavailableException(MqttMessageType connack, String clientId) {
        super(String.format("Unable to assign the state from : %s to : %s for CId=%s, close channel",
                DISCONNECTED, CONNECT_ACK, clientId));
    }
}
