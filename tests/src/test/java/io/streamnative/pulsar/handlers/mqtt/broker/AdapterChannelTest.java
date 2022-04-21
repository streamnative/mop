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
import io.netty.channel.ChannelId;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.adapter.AdapterChannel;
import io.streamnative.pulsar.handlers.mqtt.adapter.MQTTProxyAdapter;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

public class AdapterChannelTest extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();
        mqtt.setMqttProxyEnabled(true);
        mqtt.setSystemTopicEnabled(true);
        return mqtt;
    }

    @Test
    public void testAdapterChannelAutoGetConnection() throws InterruptedException {
        MQTTProxyService mockProxyService = Mockito.mock(MQTTProxyService.class);
        MQTTProxyConfiguration mqttProxyConfiguration = new MQTTProxyConfiguration();
        Mockito.when(mockProxyService.getProxyConfig()).thenReturn(mqttProxyConfiguration);
        MQTTProxyAdapter mqttProxyAdapter = new MQTTProxyAdapter(mockProxyService);
        ConcurrentMap<InetSocketAddress, Map<Integer, CompletableFuture<Channel>>> pool = mqttProxyAdapter.getPool();
        List<InetSocketAddress> proxyAddresses =
                mqttProxyPortList.stream().map(port -> new InetSocketAddress("127.0.0.1", port))
                        .collect(Collectors.toList());
        InetSocketAddress brokerAddress = proxyAddresses.get(0);
        AdapterChannel adapterChannel = mqttProxyAdapter.getAdapterChannel(brokerAddress);
        for (int i = 0; i < 20; i++) {
            Map<Integer, CompletableFuture<Channel>> brokerChannels = pool.get(brokerAddress);
            assertEquals(brokerChannels.size(), 1);
            for (Integer key : brokerChannels.keySet()) {
                CompletableFuture<Channel> channelFuture = brokerChannels.get(key);
                Channel channel = channelFuture.join();
                final ChannelId previousChannelId = channel.id();
                channel.close().sync();
                assertFalse(channel.isActive());
                String clientId = UUID.randomUUID().toString();
                MqttConnectMessage fakeConnectMessage = MqttMessageBuilders.connect()
                        .clientId(clientId).build();
                MqttAdapterMessage mqttAdapterMessage = new MqttAdapterMessage(clientId, fakeConnectMessage);
                adapterChannel.writeAndFlush(mqttAdapterMessage).join();
                CompletableFuture<Channel> channelFutureAfterSend = brokerChannels.get(key);
                Channel channelAfterSend = channelFutureAfterSend.join();
                assertNotEquals(channelAfterSend.id(), previousChannelId);
            }
        }
    }
}
