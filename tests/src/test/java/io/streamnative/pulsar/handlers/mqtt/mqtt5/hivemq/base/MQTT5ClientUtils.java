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
package io.streamnative.pulsar.handlers.mqtt.mqtt5.hivemq.base;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import java.util.Random;
import java.util.UUID;

public class MQTT5ClientUtils {
    public static Mqtt5BlockingClient createMqtt5Client(int port) {
        return Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("127.0.0.1")
                .serverPort(port)
                .buildBlocking();
    }

    public static Mqtt5BlockingClient createMqtt5ProxyClient(int proxyPort) {
        return Mqtt5Client.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("127.0.0.1")
                .serverPort(proxyPort)
                .buildBlocking();
    }

    public static void publishQos1ARandomMsg(Mqtt5BlockingClient client, String topic) {
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(("test" + new Random().nextInt(100)).getBytes())
                .send();
    }

    public static void publishQos0ARandomMsg(Mqtt5BlockingClient client, String topic) {
        client.publishWith()
                .topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .payload(("test" + new Random().nextInt(100)).getBytes())
                .send();
    }
}
