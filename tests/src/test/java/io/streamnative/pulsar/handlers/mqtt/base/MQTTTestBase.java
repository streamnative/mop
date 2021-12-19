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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.mqtt.base.provider.ClientToTopic;
import io.streamnative.pulsar.handlers.mqtt.client.MqttTestClient;
import io.streamnative.pulsar.handlers.mqtt.client.options.ClientOptions;
import io.streamnative.pulsar.handlers.mqtt.client.v3x.FuseSourceV3TestClient;
import io.streamnative.pulsar.handlers.mqtt.client.v3x.HiveMqV3TestClient;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.fusesource.mqtt.client.MQTT;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;



/**
 * Base test class for MQTT Client.
 */
@Slf4j
public class MQTTTestBase extends MQTTProtocolHandlerTestBase {

    public static final int TIMEOUT = 60 * 1000;

    private final Random random = new Random();

    @DataProvider(name = "batchEnabled")
    public Object[][] batchEnabled() {
        return new Object[][]{
                {true},
                {false}
        };
    }

    @DataProvider(name = "mqttTopicNames")
    public Object[][] mqttTopicNames() {
        return new Object[][]{
                {"a/b/c"},
                {"/a/b/c"},
                {"a/b/c/"},
                {"/a/b/c/"},
                {"persistent://public/default/t0"},
                {"persistent://public/default/a/b"},
                {"persistent://public/default//a/b"},
                {"non-persistent://public/default/t0"},
                {"non-persistent://public/default/a/b"},
                {"non-persistent://public/default//a/b"},
        };
    }

    @DataProvider(name = "mqttPersistentTopicNames")
    public Object[][] mqttPersistentTopicNames() {
        return new Object[][]{
                {"a/b/c"},
                {"/a/b/c"},
                {"a/b/c/"},
                {"/a/b/c/"},
                {"persistent://public/default/t0"},
                {"persistent://public/default/a/b"},
                {"persistent://public/default//a/b"},
        };
    }

    @DataProvider(name = "mqttTopicNameAndFilter")
    public Object[][] mqttTopicNameAndFilter() {
        return new Object[][]{
                {"a/b/c", "a/+/c"},
                {"a/b/c", "+/+/c"},
                {"a/b/c", "+/+/+"},
                {"a/b/c", "a/+/+"},
                {"a/b/c", "a/#"},
                {"/a/b/c", "/a/+/c"},
                {"/a/b/c", "/+/+/c"},
                {"/a/b/c", "/+/+/+"},
                {"/a/b/c", "/a/+/+"},
                {"/a/b/c", "/a/#"},
        };
    }

    @DataProvider(name = "mqttClient")
    public Object[][] mqttClient() {
        ClientOptions clientOptions = ClientOptions.builder()
                .clientId(UUID.randomUUID().toString())
                .serverUrl("127.0.0.1")
                .port(mqttBrokerPortList.get(random.nextInt(mqttBrokerPortList.size())))
                .build();
        return new Object[][]{
                {new HiveMqV3TestClient(clientOptions)},
                {new FuseSourceV3TestClient(clientOptions)}
        };
    }

    @DataProvider
    public Object[][] mqttClientWithPersistentTopicNames() {
        return Lists.cartesianProduct(
                        // clients
                        Arrays.stream(mqttClient())
                                .flatMap(Arrays::stream)
                                .collect(Collectors.toList()),
                        // topics
                        Arrays.stream(mqttPersistentTopicNames())
                                .flatMap(Arrays::stream)
                                .collect(Collectors.toList()))
                .stream()
                .map(inner ->
                        new Object[]{new ClientToTopic((MqttTestClient) inner.get(0), (String) inner.get(1))})
                .toArray(Object[][]::new);
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.setup();
        log.info("success internal setup");
        setupClusterNamespaces();
        setPulsarServiceState();
    }

    protected void setupClusterNamespaces() throws Exception {
        ClusterData clusterData = ClusterData.builder()
                .serviceUrl("http://127.0.0.1:" + getBrokerWebservicePortList().get(0))
                .build();
        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName, clusterData);
        } else {
            admin.clusters().updateCluster(configClusterName, clusterData);
        }
        TenantInfo tenantInfo = TenantInfo.builder()
                .adminRoles(Sets.newHashSet("appid1", "appid2"))
                .allowedClusters(Sets.newHashSet("test"))
                .build();
        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public", tenantInfo);
        } else {
            admin.tenants().updateTenant("public", tenantInfo);
        }

        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setRetention("public/default",
                    new RetentionPolicies(60, 1000));
        }
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    public MQTT createMQTTClient() throws URISyntaxException {
        List<Integer> mqttBrokerPortList = getMqttBrokerPortList();
        MQTT mqtt = new MQTT();
        mqtt.setHost("127.0.0.1", mqttBrokerPortList.get(random.nextInt(mqttBrokerPortList.size())));
        return mqtt;
    }

    public MQTT createMQTTTlsClient() throws URISyntaxException {
        List<Integer> mqttBrokerPortTlsList = getMqttBrokerPortTlsList();
        MQTT mqtt = new MQTT();
        mqtt.setHost(URI.create("ssl://127.0.0.1:"
                + mqttBrokerPortTlsList.get(random.nextInt(mqttBrokerPortTlsList.size()))));
        return mqtt;
    }

    public MQTT createMQTTProxyClient() throws URISyntaxException {
        List<Integer> mqttProxyPortList = getMqttProxyPortList();
        MQTT mqtt = createMQTTClient();
        mqtt.setHost("127.0.0.1", mqttProxyPortList.get(random.nextInt(mqttProxyPortList.size())));
        return mqtt;
    }

    public MQTT createMQTTProxyTlsClient() throws URISyntaxException {
        List<Integer> mqttProxyPortTlsList = getMqttProxyPortTlsList();
        MQTT mqtt = createMQTTClient();
        mqtt.setHost(URI.create("ssl://127.0.0.1:"
                + mqttProxyPortTlsList.get(random.nextInt(mqttProxyPortTlsList.size()))));
        return mqtt;
    }
}
