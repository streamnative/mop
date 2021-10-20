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

import static org.mockito.Mockito.spy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderBasic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.MQTTException;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Basic authentication proxy test.
 */
@Slf4j
public class BasicAuthorizationProxyTest extends MQTTTestBase {

    @Override
    public MQTTServerConfiguration initConfig() throws Exception{
        MQTTServerConfiguration conf = super.initConfig();
        System.setProperty("pulsar.auth.basic.conf", "./src/test/resources/htpasswd");
        String authParams = "{\"userId\":\"superUser\",\"password\":\"supepass\"}";

        conf.setAuthenticationEnabled(true);
        conf.setMqttAuthenticationEnabled(true);
        conf.setMqttAuthorizationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setMqttAuthenticationMethods(ImmutableList.of("basic"));
        conf.setSuperUserRoles(ImmutableSet.of("superUser"));
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderBasic.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationBasic.class.getName());
        conf.setBrokerClientAuthenticationParameters(authParams);

        return conf;
    }

    @Override
    public void afterSetup() throws Exception {
        String authParams = "{\"userId\":\"superUser\",\"password\":\"supepass\"}";
        AuthenticationBasic authPassword = new AuthenticationBasic();
        authPassword.configure(authParams);
        pulsarClient = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .authentication(authPassword)
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        admin = spy(PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl.toString())
                .authentication(authPassword)
                .build());

    }

    @Test(timeOut = TIMEOUT)
    public void testPubAndSubWithAuthorized() throws Exception {
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
    }

    @Test(expectedExceptions = {MQTTException.class}, timeOut = TIMEOUT)
    public void testNotAuthorized() throws Exception {
        MQTT mqtt = createMQTTClient();
        mqtt.setUserName("user1");
        mqtt.setPassword("pass2");
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        String message = "Hello MQTT";
        connection.publish("a", message.getBytes(), QoS.AT_MOST_ONCE, false);
    }
}
