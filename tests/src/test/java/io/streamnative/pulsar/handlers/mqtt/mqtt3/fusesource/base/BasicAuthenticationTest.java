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
package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.base;

import com.hivemq.client.internal.shaded.org.jetbrains.annotations.NotNull;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientConfig;
import com.hivemq.client.mqtt.mqtt5.auth.Mqtt5EnhancedAuthMechanism;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5Auth;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5AuthBuilder;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5EnhancedAuthBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5Disconnect;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.pulsar.handlers.mqtt.base.BasicAuthenticationConfig;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.MQTTException;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Basic authentication test.
 */
@Slf4j
public class BasicAuthenticationTest extends BasicAuthenticationConfig {

    @Test(timeOut = TIMEOUT)
    public void testAuthenticate() throws Exception {
        MQTT mqtt = createMQTTClient();
        String topicName = "persistent://public/default/testAuthentication";
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = {new Topic(topicName, QoS.AT_LEAST_ONCE)};
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(received.getTopic(), topicName);
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testAuthenticateWithAuthMethod() throws Exception {
        String topic = "persistent://public/default/testAuthenticateWithAuthMethod";
        Mqtt5BlockingClient client1 = Mqtt5Client.builder()
                .identifier("abc")
                .serverHost("127.0.0.1")
                .serverPort(getMqttBrokerPortList().get(0))
                .buildBlocking();
        client1.connectWith().enhancedAuth(new Mqtt5EnhancedAuthMechanism() {
            @Override
            public @NotNull MqttUtf8String getMethod() {
                return MqttUtf8String.of("basic");
            }

            @Override
            public int getTimeout() {
                return 10;
            }

            @Override
            public @NotNull CompletableFuture<Void> onAuth(@NotNull Mqtt5ClientConfig clientConfig,
                                                           @NotNull Mqtt5Connect connect,
                                                           @NotNull Mqtt5EnhancedAuthBuilder authBuilder) {
                authBuilder.data("superUser:supepass".getBytes(StandardCharsets.UTF_8));
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public @NotNull CompletableFuture<Void> onReAuth(@NotNull Mqtt5ClientConfig clientConfig,
                                                             @NotNull Mqtt5AuthBuilder authBuilder) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public @NotNull CompletableFuture<Boolean> onContinue(@NotNull Mqtt5ClientConfig clientConfig,
                                                                  @NotNull Mqtt5Auth auth,
                                                                  @NotNull Mqtt5AuthBuilder authBuilder) {
                return CompletableFuture.completedFuture(false);
            }

            @Override
            public @NotNull CompletableFuture<Boolean> onAuthSuccess(@NotNull Mqtt5ClientConfig clientConfig,
                                                                     @NotNull Mqtt5ConnAck connAck) {
                return CompletableFuture.completedFuture(true);
            }

            @Override
            public @NotNull CompletableFuture<Boolean> onReAuthSuccess(@NotNull Mqtt5ClientConfig clientConfig,
                                                                       @NotNull Mqtt5Auth auth) {
                return null;
            }

            @Override
            public void onAuthRejected(@NotNull Mqtt5ClientConfig clientConfig, @NotNull Mqtt5ConnAck connAck) {
            }

            @Override
            public void onReAuthRejected(@NotNull Mqtt5ClientConfig clientConfig, @NotNull Mqtt5Disconnect disconnect) {
            }

            @Override
            public void onAuthError(@NotNull Mqtt5ClientConfig clientConfig, @NotNull Throwable cause) {
            }

            @Override
            public void onReAuthError(@NotNull Mqtt5ClientConfig clientConfig, @NotNull Throwable cause) {
            }
        }).send();
        Mqtt5Publish publishMessage = Mqtt5Publish.builder().topic(topic)
                .qos(MqttQos.AT_LEAST_ONCE).build();
        client1.subscribeWith()
                .topicFilter(topic)
                .qos(MqttQos.AT_LEAST_ONCE)
                .send();
        Mqtt5BlockingClient.Mqtt5Publishes publishes = client1.publishes(MqttGlobalPublishFilter.ALL);
        client1.publish(publishMessage);
        Mqtt5Publish message = publishes.receive();
        Assert.assertNotNull(message);
        publishes.close();
        client1.disconnect();
    }

    @Test(expectedExceptions = {MQTTException.class}, timeOut = TIMEOUT)
    public void testNoAuthenticated() throws Exception {
        MQTT mqtt = createMQTTClient();
        mqtt.setUserName("user1");
        mqtt.setPassword("invalid");
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
    }
}
