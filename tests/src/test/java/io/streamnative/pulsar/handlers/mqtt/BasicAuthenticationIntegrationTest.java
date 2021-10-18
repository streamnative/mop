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
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderBasic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.MQTTException;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Basic authentication integration tests for MQTT protocol handler.
 */
@Slf4j
public class BasicAuthenticationIntegrationTest extends MQTTTestBase {

    @Override
    public MQTTServerConfiguration initConfig() throws Exception{
        MQTTServerConfiguration conf = super.initConfig();
        System.setProperty("pulsar.auth.basic.conf", "./src/test/resources/htpasswd");
        String authParams = "{\"userId\":\"superUser\",\"password\":\"supepass\"}";

        conf.setMqttProxyEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setMqttAuthenticationEnabled(true);
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

    @Override
    public MQTT createMQTTClient() throws URISyntaxException {
        MQTT mqtt = super.createMQTTClient();
        mqtt.setUserName("superUser");
        mqtt.setPassword("supepass");
        return mqtt;
    }

    @Test(timeOut = TIMEOUT)
    public void testAuthenticateAndPublish() throws Exception {
        MQTT mqtt = createMQTTClient();
        authenticateAndPublish(mqtt);
    }

    @Test(timeOut = TIMEOUT)
    public void testAuthenticateAndPublishViaProxy() throws Exception {
        MQTT mqtt = createMQTTProxyClient();
        authenticateAndPublish(mqtt);
    }

    public void authenticateAndPublish(MQTT mqtt) throws Exception {
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

    @Test(expectedExceptions = {MQTTException.class}, timeOut = TIMEOUT)
    public void testInvalidCredentials() throws Exception {
        MQTT mqtt = createMQTTClient();
        mqtt.setPassword("invalid");
        BlockingConnection connection = mqtt.blockingConnection();
        connection.connect();
        connection.disconnect();
    }
}
