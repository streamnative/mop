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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * Unit test to test MoP handler.
 */
@Slf4j
public abstract class MQTTProtocolHandlerTestBase {

    public static final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    public static final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
    public static final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/certificate/client.crt";
    public static final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/certificate/client.key";

    protected PulsarAdmin admin;
    protected URL brokerUrl;
    protected URL brokerUrlTls;
    protected URI lookupUrl;
    protected PulsarClient pulsarClient;
    // broker2
    protected PulsarAdmin admin2;
    protected URL brokerUrl2;
    protected URL brokerUrlTls2;
    protected URI lookupUrl2;
    protected PulsarClient pulsarClient2;
    // broker3
    protected PulsarAdmin admin3;
    protected URL brokerUrl3;
    protected URL brokerUrlTls3;
    protected URI lookupUrl3;
    protected PulsarClient pulsarClient3;

    protected String defaultTenant = "public";
    protected String defaultNamespace = "default";
    protected TopicDomain defaultTopicDomain = TopicDomain.persistent;

    @Getter
    protected int mqttBrokerPortTls = PortManager.nextFreePort();

    protected int brokerCount = 1;

    @Getter
    protected List<Integer> brokerWebservicePortList = new ArrayList<>();
    @Getter
    protected List<Integer> brokerWebServicePortTlsList = new ArrayList<>();
    @Getter
    protected List<PulsarService> pulsarServiceList = new ArrayList<>();
    @Getter
    protected List<Integer> brokerPortList = new ArrayList<>();
    @Getter
    protected List<Integer> mqttBrokerPortList = new ArrayList<>();
    @Getter
    protected List<Integer> mqttBrokerPortTlsList = new ArrayList<>();
    @Getter
    protected List<Integer> mqttBrokerPortTlsPskList = new ArrayList<>();
    @Getter
    protected List<Integer> mqttProxyPortList = new ArrayList<>();
    @Getter
    protected List<Integer> mqttProxyPortTlsList = new ArrayList<>();
    @Getter
    protected List<Integer> mqttProxyPortTlsPskList = new ArrayList<>();

    protected List<PulsarAdmin> adminList = new ArrayList<>();
    protected List<PulsarClient> clientList = new ArrayList<>();

    protected MockZooKeeper mockZooKeeper;
    protected NonClosableMockBookKeeper mockBookKeeper;
    protected boolean isTcpLookup = false;
    protected final String configClusterName = "test";
    protected boolean enableTls = false;

    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private OrderedExecutor bkExecutor;

    protected MQTTCommonConfiguration initConfig() throws Exception{
        MQTTCommonConfiguration mqtt = new MQTTCommonConfiguration();
        mqtt.setAdvertisedAddress("localhost");
        mqtt.setClusterName(configClusterName);

        mqtt.setManagedLedgerCacheSizeMB(8);
        mqtt.setActiveConsumerFailoverDelayTimeMillis(0);
        mqtt.setDefaultRetentionTimeInMinutes(7);
        mqtt.setDefaultNumberOfNamespaceBundles(4);
        mqtt.setZookeeperServers("localhost:2181");
        mqtt.setConfigurationStoreServers("localhost:3181");

        mqtt.setAuthenticationEnabled(false);
        mqtt.setAuthorizationEnabled(false);
        mqtt.setAllowAutoTopicCreation(true);
        mqtt.setBrokerDeleteInactiveTopicsEnabled(false);
        mqtt.setAcknowledgmentAtBatchIndexLevelEnabled(true);

        // set protocol related config
        URL testHandlerUrl = this.getClass().getClassLoader().getResource("test-protocol-handler.nar");
        Path handlerPath;
        try {
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            throw e;
        }

        String protocolHandlerDir = handlerPath.toFile().getParent();

        mqtt.setProtocolHandlerDirectory(
            protocolHandlerDir
        );
        mqtt.setMessagingProtocols(Sets.newHashSet("mqtt"));
        mqtt.setAdditionalServlets(Sets.newHashSet("mqtt-servlet"));
        mqtt.setAdditionalServletDirectory(protocolHandlerDir);
        return mqtt;
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(url).statsInterval(intervalInSecs, TimeUnit.SECONDS).build();
    }

    protected void setup() throws Exception {
        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        bkExecutor = OrderedExecutor.newBuilder().numThreads(1).name("mock-pulsar-bk").build();

        mockZooKeeper = createMockZooKeeper();
        mockBookKeeper = createMockBookKeeper(bkExecutor);

        startBroker();

        brokerUrl = new URL("http://" + pulsarServiceList.get(0).getAdvertisedAddress() + ":"
                + pulsarServiceList.get(0).getConfiguration().getWebServicePort().get());
        brokerUrlTls = new URL("https://" + pulsarServiceList.get(0).getAdvertisedAddress() + ":"
                + pulsarServiceList.get(0).getConfiguration().getWebServicePortTls().get());

        lookupUrl = new URI(brokerUrl.toString());
        if (isTcpLookup) {
            lookupUrl = new URI("broker://localhost:" + brokerPortList.get(0));
        }

        pulsarClient = newPulsarClient(lookupUrl.toString(), 0);
        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).build());
        adminList.add(admin);
        clientList.add(pulsarClient);
        if (pulsarServiceList.size() > 1) {
            brokerUrl2 = new URL("http://" + pulsarServiceList.get(1).getAdvertisedAddress() + ":"
                    + pulsarServiceList.get(1).getConfiguration().getWebServicePort().get());
            brokerUrlTls2 = new URL("https://" + pulsarServiceList.get(1).getAdvertisedAddress() + ":"
                    + pulsarServiceList.get(1).getConfiguration().getWebServicePortTls().get());

            lookupUrl2 = new URI(brokerUrl2.toString());
            if (isTcpLookup) {
                lookupUrl2 = new URI("broker://localhost:" + brokerPortList.get(1));
            }

            pulsarClient2 = newPulsarClient(lookupUrl2.toString(), 0);
            admin2 = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl2.toString()).build());
            adminList.add(admin2);
            clientList.add(pulsarClient2);
            //
            brokerUrl3 = new URL("http://" + pulsarServiceList.get(2).getAdvertisedAddress() + ":"
                    + pulsarServiceList.get(2).getConfiguration().getWebServicePort().get());
            brokerUrlTls3 = new URL("https://" + pulsarServiceList.get(2).getAdvertisedAddress() + ":"
                    + pulsarServiceList.get(2).getConfiguration().getWebServicePortTls().get());

            lookupUrl3 = new URI(brokerUrl3.toString());
            if (isTcpLookup) {
                lookupUrl3 = new URI("broker://localhost:" + brokerPortList.get(2));
            }

            pulsarClient3 = newPulsarClient(lookupUrl3.toString(), 0);
            admin3 = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl3.toString()).build());
            adminList.add(admin3);
            clientList.add(pulsarClient3);
        }

        afterSetup();
    }

    protected void afterSetup() throws Exception {
        //NOP
    }

    protected final void internalCleanup() throws Exception {
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            adminList.forEach(admin -> admin.close());
            for (PulsarClient client : clientList) {
                client.close();
            }
            if (pulsarServiceList != null && !pulsarServiceList.isEmpty()) {
                stopBroker();
            }
            if (mockBookKeeper != null) {
                mockBookKeeper.reallyShutdown();
            }
            if (mockZooKeeper != null) {
                mockZooKeeper.shutdown();
            }
            if (sameThreadOrderedSafeExecutor != null) {
                sameThreadOrderedSafeExecutor.shutdown();
            }
            if (bkExecutor != null) {
                bkExecutor.shutdown();
            }
        } catch (Exception e) {
            log.warn("Failed to clean up mocked pulsar service:", e);
            throw e;
        }
    }

    protected void cleanup() throws Exception {
        internalCleanup();
    }

    protected void restartBroker() throws Exception {
        stopBroker();

        startBroker();
    }

    protected void stopBroker() throws Exception {
        for (PulsarService pulsarService : pulsarServiceList) {
            pulsarService.close();
        }
        pulsarServiceList.clear();
        brokerPortList.clear();
        brokerWebservicePortList.clear();
        brokerWebServicePortTlsList.clear();
        mqttBrokerPortList.clear();
        mqttBrokerPortTlsList.clear();
        mqttBrokerPortTlsPskList.clear();
        mqttProxyPortList.clear();
        mqttProxyPortTlsList.clear();
        mqttProxyPortTlsPskList.clear();
    }

    public void stopBroker(int brokerIndex) throws Exception {
        pulsarServiceList.get(brokerIndex).close();
        pulsarServiceList.remove(brokerIndex);
        brokerPortList.remove(brokerIndex);
        brokerWebservicePortList.remove(brokerIndex);
        brokerWebServicePortTlsList.remove(brokerIndex);
        mqttBrokerPortList.remove(brokerIndex);
        mqttBrokerPortTlsList.remove(brokerIndex);
        mqttBrokerPortTlsPskList.remove(brokerIndex);
        mqttProxyPortList.remove(brokerIndex);
        mqttProxyPortTlsList.remove(brokerIndex);
        mqttProxyPortTlsPskList.remove(brokerIndex);
    }

    protected void startBroker() throws Exception {
        MQTTCommonConfiguration conf = initConfig();
        if (conf.isMqttProxyEnabled()) {
            brokerCount = 3;
        }
        for (int i = 0; i < brokerCount; i++) {
            MQTTCommonConfiguration brokerConf = new MQTTCommonConfiguration();
            BeanUtils.copyProperties(brokerConf, conf);
            brokerConf.setProperties(conf.getProperties());
            startBroker(brokerConf);
        }
    }

    protected void startBroker(MQTTCommonConfiguration conf) throws Exception {
        int brokerPort = PortManager.nextFreePort();
        brokerPortList.add(brokerPort);

        int brokerPortTls = PortManager.nextFreePort();
        brokerPortList.add(brokerPortTls);

        int brokerWebServicePort = PortManager.nextFreePort();
        brokerWebservicePortList.add(brokerWebServicePort);

        int brokerWebServicePortTls = PortManager.nextFreePort();
        brokerWebServicePortTlsList.add(brokerWebServicePortTls);

        int mqttBrokerPort = PortManager.nextFreePort();
        mqttBrokerPortList.add(mqttBrokerPort);

        int mqttBrokerTlsPort = -1;
        if (enableTls) {
            mqttBrokerTlsPort = PortManager.nextFreePort();
            mqttBrokerPortTlsList.add(mqttBrokerTlsPort);
        }
        int mqttBrokerTlsPskPort = -1;
        if (conf.isMqttTlsPskEnabled()) {
            mqttBrokerTlsPskPort = PortManager.nextFreePort();
            mqttBrokerPortTlsPskList.add(mqttBrokerTlsPskPort);
        }

        int mqttProxyPort = -1;
        int mqttProxyTlsPort = -1;
        int mqttProxyTlsPskPort = -1;
        if (conf.isMqttProxyEnabled()) {
            mqttProxyPort = PortManager.nextFreePort();
            conf.setMqttProxyPort(mqttProxyPort);
            mqttProxyPortList.add(mqttProxyPort);
            if (conf.isMqttProxyTlsEnabled()) {
                mqttProxyTlsPort = PortManager.nextFreePort();
                conf.setMqttProxyTlsPort(mqttProxyTlsPort);
                mqttProxyPortTlsList.add(mqttProxyTlsPort);
            }
            if (conf.isMqttProxyTlsPskEnabled()) {
                mqttProxyTlsPskPort = PortManager.nextFreePort();
                conf.setMqttProxyTlsPskPort(mqttProxyTlsPskPort);
                mqttProxyPortTlsPskList.add(mqttProxyTlsPskPort);
            }
        }

        conf.setBrokerServicePort(Optional.of(brokerPort));
        conf.setBrokerServicePortTls(Optional.of(brokerPortTls));
        conf.setWebServicePort(Optional.of(brokerWebServicePort));
        conf.setWebServicePortTls(Optional.of(brokerWebServicePortTls));
        String listener = "mqtt://127.0.0.1:" + mqttBrokerPort;
        String tlsListener = null;
        String tlsPskListener = null;
        if (enableTls) {
            tlsListener = "mqtt+ssl://127.0.0.1:" + mqttBrokerTlsPort;
        }
        if (conf.isMqttTlsPskEnabled()) {
            tlsPskListener = "mqtt+ssl+psk://127.0.0.1:" + mqttBrokerTlsPskPort;
        }
        conf.getProperties().setProperty("mqttListeners",
                Joiner.on(",").skipNulls().join(listener, tlsListener, tlsPskListener));

        log.info("Start broker info, brokerPort: {}, brokerPortTls : {}, "
                        + "brokerWebServicePort : {} , brokerWebServicePortTls : {}, "
                        + "mqttBrokerPort: {}, mqttBrokerTlsPort: {}, mqttBrokerTlsPskPort: {}, "
                        + "mqttProxyPort: {}, mqttProxyTlsPort: {}, mqttProxyTlsPskPort: {}",
                brokerPort, brokerPortTls, brokerWebServicePort, brokerWebServicePortTls,
                mqttBrokerPort, mqttBrokerTlsPort, mqttBrokerTlsPskPort,
                mqttProxyPort, mqttProxyTlsPort, mqttProxyTlsPskPort);
        ConfigurationUtils.extractFieldToProperties(conf);
        this.pulsarServiceList.add(doStartBroker(conf));
    }

    protected PulsarService doStartBroker(ServiceConfiguration conf) throws Exception {
        PulsarService pulsar = spy(new PulsarService(conf));

        setupBrokerMocks(pulsar);
        pulsar.start();

        return pulsar;
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(pulsar).createLocalMetadataStore(any());
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(pulsar).createConfigurationMetadataStore(any());

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        zk.setSessionId(-1);
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
            "".getBytes(StandardCharsets.UTF_8), dummyAclList, CreateMode.PERSISTENT);

        zk.create(
            "/ledgers/LAYOUT",
            "1\nflat:1".getBytes(StandardCharsets.UTF_8), dummyAclList,
            CreateMode.PERSISTENT);
        return zk;
    }

    public static NonClosableMockBookKeeper createMockBookKeeper(OrderedExecutor executor) throws Exception {
        return spy(new NonClosableMockBookKeeper(executor));
    }

    /**
     * Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test.
     */
    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(OrderedExecutor executor) throws Exception {
            super(executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }

    private BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public CompletableFuture<BookKeeper> create(ServiceConfiguration conf, MetadataStoreExtended store,
                                                    EventLoopGroup eventLoopGroup,
            Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
            Map<String, Object> ensemblePlacementPolicyProperties) {
            return CompletableFuture.completedFuture(mockBookKeeper);
        }

        @Override
        public CompletableFuture<BookKeeper> create(ServiceConfiguration conf, MetadataStoreExtended store,
                                                    EventLoopGroup eventLoopGroup,
            Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
            Map<String, Object> ensemblePlacementPolicyProperties, StatsLogger statsLogger) {
            return CompletableFuture.completedFuture(mockBookKeeper);
        }

        @Override
        public void close() {

        }
    };

    public static void retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis)
        throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null) || i == (retryCount - 1)) {
                break;
            }
            Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
    }

    public static void setFieldValue(Class clazz, Object classObj, String fieldName, Object fieldValue)
        throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(classObj, fieldValue);
    }

    public void setPulsarServiceState() {
        for (PulsarService pulsarService : pulsarServiceList) {
            doReturn(PulsarService.State.Started).when(pulsarService).getState();
        }
    }

    /**
     * Set the starting broker count for test.
     */
    public void setBrokerCount(int brokerCount) {
        this.brokerCount = brokerCount;
    }

    public static String randExName() {
        return randomName("ex-", 4);
    }

    public static String randQuName() {
        return randomName("qu-", 4);
    }

    public static String randomName(String prefix, int numChars) {
        StringBuilder sb = new StringBuilder(prefix);
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

}
