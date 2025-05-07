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

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.mqtt.base.AuthorizationConfig;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.GrantTopicPermissionOptions;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.awaitility.Awaitility;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;
/**
 * Authorization test.
 */
@Slf4j
public class AuthorizationTest extends AuthorizationConfig {

    @Test(timeOut = TIMEOUT)
    public void testAuthorizedOnNamespace() throws Exception {
        Set<AuthAction> user1Actions = new HashSet<>();
        user1Actions.add(AuthAction.produce);
        admin.namespaces().grantPermissionOnNamespace("public/default", "user1", user1Actions);

        Set<AuthAction> user2Actions = new HashSet<>();
        user2Actions.add(AuthAction.consume);
        admin.namespaces().grantPermissionOnNamespace("public/default", "user2", user2Actions);

        String topicName = "persistent://public/default/testAuthorization";
        MQTT mqttConsumer = createMQTTClient();
        mqttConsumer.setUserName("user2");
        mqttConsumer.setPassword("pass2");
        BlockingConnection consumer = mqttConsumer.blockingConnection();
        consumer.connect();
        Topic[] topics = {new Topic(topicName, QoS.AT_LEAST_ONCE)};
        consumer.subscribe(topics);

        MQTT mqttProducer = createMQTTClient();
        mqttProducer.setUserName("user1");
        mqttProducer.setPassword("pass1");
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        String message = "Hello MQTT";
        producer.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);

        Message receive = consumer.receive();
        Assert.assertEquals(new String(receive.getPayload()), message);
        producer.disconnect();
        consumer.disconnect();
    }

    @Test(timeOut = TIMEOUT)
    public void testAuthorizedOnTopic() throws Exception {
        String topicName = "persistent://public/default/testAuthorizedOnTopic/a";
        String encodedTopicName = "persistent://public/default/" + URLEncoder.encode("testAuthorizedOnTopic/a");
        admin.topics().createNonPartitionedTopic(encodedTopicName);
        Set<AuthAction> user1Actions = new HashSet<>();
        user1Actions.add(AuthAction.produce);
        final GrantTopicPermissionOptions permission1 = GrantTopicPermissionOptions.builder()
                .topic(encodedTopicName)
                .role("user1")
                .actions(user1Actions)
                .build();
        admin.namespaces().grantPermissionOnTopics(Lists.newArrayList(permission1));

        Set<AuthAction> user2Actions = new HashSet<>();
        user2Actions.add(AuthAction.consume);
        final GrantTopicPermissionOptions permission2 = GrantTopicPermissionOptions.builder()
                .topic(encodedTopicName)
                .role("user2")
                .actions(user2Actions)
                .build();
        admin.namespaces().grantPermissionOnTopics(Lists.newArrayList(permission2));

        MQTT mqttConsumer = createMQTTClient();
        mqttConsumer.setUserName("user2");
        mqttConsumer.setPassword("pass2");
        BlockingConnection consumer = mqttConsumer.blockingConnection();
        consumer.connect();
        Topic[] topics = {new Topic(topicName, QoS.AT_LEAST_ONCE)};
        consumer.subscribe(topics);

        MQTT mqttProducer = createMQTTClient();
        mqttProducer.setUserName("user1");
        mqttProducer.setPassword("pass1");
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        String message = "Hello MQTT";
        producer.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);

        Message receive = consumer.receive();
        Assert.assertEquals(new String(receive.getPayload()), message);
        producer.disconnect();
        consumer.disconnect();
    }

    @Test
    public void testNotAuthorized() throws Exception {
        Set<AuthAction> user3Actions = new HashSet<>();
        user3Actions.add(AuthAction.consume);
        admin.namespaces().grantPermissionOnNamespace("public/default", "user3", user3Actions);
        MQTT mqtt = createMQTTClient();
        mqtt.setUserName("user3");
        mqtt.setPassword("pass3");
        mqtt.setConnectAttemptsMax(0);
        mqtt.setReconnectAttemptsMax(0);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        String message = "Hello MQTT";
        connection.publish("a", message.getBytes(), QoS.AT_MOST_ONCE, false);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertFalse(connection.isConnected());
        });
    }
}
