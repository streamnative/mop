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
package io.streamnative.pulsar.handlers.mqtt;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTProtocolHandlerTestBase;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Simple integration tests for MQTT protocol handler.
 */
@Slf4j
public class SimpleIntegrationTest extends MQTTProtocolHandlerTestBase {
    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }
        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }

        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setRetention("public/default",
                    new RetentionPolicies(60, 1000));
        }
        Mockito.when(pulsar.getState()).thenReturn(PulsarService.State.Started);
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSimpleMqttPubAndSubQos0() throws Exception {
        final String topicName = "persistent://public/default/qos0";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", 1883);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_MOST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_MOST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test
    public void testSimpleMqttPubAndSubQos1() throws Exception {
        final String topicName = "persistent://public/default/qos1";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", 1883);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);
        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);
        Message received = connection.receive();
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
    }

    @Test
    public void testSendByMqttAndReceiveByPulsar() throws Exception {
        final String topicName = "persistent://public/default/testReceiveByPulsar";
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscribe();

        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", 1883);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();

        String message = "Hello MQTT";
        connection.publish(topicName, message.getBytes(), QoS.AT_LEAST_ONCE, false);

        org.apache.pulsar.client.api.Message<byte[]> received = consumer.receive();
        Assert.assertNotNull(received);
        Assert.assertEquals(new String(received.getValue()), message);
        consumer.acknowledge(received);

        consumer.close();
        connection.disconnect();
    }

    @Test
    public void testSendByPulsarAndReceiveByMqtt() throws Exception {
        final String topicName = "persistent://public/default/testSendByPulsarAndReceiveByMqtt";
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", 1883);
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        Topic[] topics = { new Topic(topicName, QoS.AT_LEAST_ONCE) };
        connection.subscribe(topics);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();

        String message = "Hello MQTT";

        producer.newMessage().value(message).send();
        Message received = connection.receive();
        System.out.println(Arrays.toString(message.getBytes()));
        System.out.println(Arrays.toString(received.getPayload()));
        Assert.assertEquals(new String(received.getPayload()), message);
        received.ack();
        connection.disconnect();
        producer.close();
    }
}
