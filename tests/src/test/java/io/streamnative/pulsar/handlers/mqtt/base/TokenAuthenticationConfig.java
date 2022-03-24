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
package io.streamnative.pulsar.handlers.mqtt.base;

import static org.mockito.Mockito.spy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.fusesource.mqtt.client.MQTT;

/**
 * Token authentication config.
 */
@Slf4j
public class TokenAuthenticationConfig extends MQTTTestBase {
    private String token;

    @Override
    public MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration conf = super.initConfig();
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        token = AuthTokenUtils.createToken(secretKey, "superUser", Optional.empty());

        conf.setAuthenticationEnabled(true);
        conf.setMqttAuthenticationEnabled(true);
        conf.setMqttAuthenticationMethods(ImmutableList.of("token"));
        conf.setSuperUserRoles(ImmutableSet.of("superUser"));
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + token);
        conf.setProperties(properties);

        return conf;
    }

    @Override
    public void afterSetup() throws Exception {
        AuthenticationToken authToken = new AuthenticationToken();
        authToken.configure("token:" + token);

        pulsarClient = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .authentication(authToken)
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        admin = spy(PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl.toString())
                .authentication(authToken)
                .build());
    }

    @Override
    public MQTT createMQTTClient() throws URISyntaxException {
        MQTT mqtt = super.createMQTTClient();
        mqtt.setUserName("superUser");
        mqtt.setPassword(token);
        return mqtt;
    }
}
