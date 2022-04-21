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
package io.streamnative.pulsar.handlers.mqtt.broker;

import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.adapter.MQTTProxyAdapter;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MqttProxyAdapterTest extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();
        mqtt.setMqttProxyEnabled(true);
        mqtt.setSystemTopicEnabled(true);
        return mqtt;
    }

    @Test
    public void testProxyAdapterConnectionPool() {
        MQTTProxyService mockProxyService = Mockito.mock(MQTTProxyService.class);
        MQTTProxyConfiguration mqttProxyConfiguration = new MQTTProxyConfiguration();
        Mockito.when(mockProxyService.getProxyConfig()).thenReturn(mqttProxyConfiguration);
        MQTTProxyAdapter mqttProxyAdapter = new MQTTProxyAdapter(mockProxyService);
        List<InetSocketAddress> proxyAddresses =
                mqttProxyPortList.stream().map(port -> new InetSocketAddress("127.0.0.1", port))
                .collect(Collectors.toList());
        for (InetSocketAddress proxyAddress : proxyAddresses) {
            mqttProxyAdapter.getAdapterChannel(proxyAddress);
        }
        ConcurrentMap<InetSocketAddress, Map<Integer, CompletableFuture<Channel>>> pool = mqttProxyAdapter.getPool();
        assertEquals(pool.size(), 3);
        for (InetSocketAddress proxyAddress : proxyAddresses) {
            Map<Integer, CompletableFuture<Channel>> brokerChannels = pool.get(proxyAddress);
            assertEquals(brokerChannels.size(), 1);
            Optional<CompletableFuture<Channel>> channelFutureOpt = brokerChannels.values()
                    .stream()
                    .findAny();
            assertTrue(channelFutureOpt.isPresent());
            CompletableFuture<Channel> channelFuture = channelFutureOpt.get();
            Channel channel = channelFuture.join();
            assertTrue(channel.isActive());
        }
    }

    @Test
    public void testProxyAdapterCloseInvokeCleanup() throws InterruptedException {
        MQTTProxyService mockProxyService = Mockito.mock(MQTTProxyService.class);
        MQTTProxyConfiguration mqttProxyConfiguration = new MQTTProxyConfiguration();
        Mockito.when(mockProxyService.getProxyConfig()).thenReturn(mqttProxyConfiguration);
        MQTTProxyAdapter mqttProxyAdapter = new MQTTProxyAdapter(mockProxyService);
        List<InetSocketAddress> proxyAddresses =
                mqttProxyPortList.stream().map(port -> new InetSocketAddress("127.0.0.1", port))
                        .collect(Collectors.toList());
        for (InetSocketAddress proxyAddress : proxyAddresses) {
            mqttProxyAdapter.getAdapterChannel(proxyAddress);
        }
        ConcurrentMap<InetSocketAddress, Map<Integer, CompletableFuture<Channel>>> pool = mqttProxyAdapter.getPool();
        assertEquals(pool.size(), 3);
        for (InetSocketAddress proxyAddress : proxyAddresses) {
            Map<Integer, CompletableFuture<Channel>> brokerChannels = pool.get(proxyAddress);
            assertEquals(brokerChannels.size(), 1);
            for (CompletableFuture<Channel> channelFuture : brokerChannels.values()) {
                Channel channel = channelFuture.join();
                assertTrue(channel.isActive());
                channel.close().sync();
                assertFalse(channel.isActive());
                assertEquals(brokerChannels.size(), 0);
            }
        }
    }

    @Test
    public void testConfigurableConnectionPoolSize() {
        final int channelNumPerBroker = 10;
        MQTTProxyService mockProxyService = Mockito.mock(MQTTProxyService.class);
        MQTTProxyConfiguration mqttProxyConfiguration = new MQTTProxyConfiguration();
        mqttProxyConfiguration.setMqttProxyMaxNoOfChannels(channelNumPerBroker);
        Mockito.when(mockProxyService.getProxyConfig()).thenReturn(mqttProxyConfiguration);
        MQTTProxyAdapter mqttProxyAdapter = new MQTTProxyAdapter(mockProxyService);
        List<InetSocketAddress> proxyAddresses =
                mqttProxyPortList.stream().map(port -> new InetSocketAddress("127.0.0.1", port))
                        .collect(Collectors.toList());
        ConcurrentMap<InetSocketAddress, Map<Integer, CompletableFuture<Channel>>> pool = mqttProxyAdapter.getPool();
        for (InetSocketAddress proxyAddress : proxyAddresses) {
            Awaitility.await()
                    .untilAsserted(()-> {
                        mqttProxyAdapter.getAdapterChannel(proxyAddress);
                        assertEquals(pool.get(proxyAddress).size(), channelNumPerBroker);
                    });
        }
        for (InetSocketAddress proxyAddress : proxyAddresses) {
            Map<Integer, CompletableFuture<Channel>> brokerChannels = pool.get(proxyAddress);
            assertEquals(brokerChannels.size(), channelNumPerBroker);
            for (CompletableFuture<Channel> channelFuture : brokerChannels.values()) {
                Channel channel = channelFuture.join();
                assertTrue(channel.isActive());
            }
        }
    }

}
