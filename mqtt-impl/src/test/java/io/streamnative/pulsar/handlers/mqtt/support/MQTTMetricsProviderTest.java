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
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class MQTTMetricsProviderTest {

    private MQTTServerConfiguration serverConfiguration;
    private SimpleTextOutputStream outputStream;
    private ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();

    @BeforeMethod
    public void setUp() {
        outputStream = new SimpleTextOutputStream(buf);
        serverConfiguration = mock(MQTTServerConfiguration.class);
        doReturn("mop-cluster").when(serverConfiguration).getClusterName();
    }

    @Test
    public void testGenerate() {
        MQTTMetricsProvider provider = new MQTTMetricsProvider(serverConfiguration);
        provider.addClient("192.168.1.0:11022");
        provider.generate(outputStream);
        String result = new String(buf.array(), buf.arrayOffset(), buf.readableBytes());
        buf.release();
        Assert.assertTrue(result.contains("mop_online_clients_count"));
    }

    @Test
    public void testClientsData() {
        MQTTMetricsProvider provider = new MQTTMetricsProvider(serverConfiguration);
        String client1 = "192.168.1.0:11022";
        provider.addClient(client1);
        Assert.assertTrue(provider.getOnlineClients().contains(client1));
        Assert.assertEquals(provider.getOnlineClientsCount(), 1);
        provider.addClient(client1);
        Assert.assertEquals(provider.getOnlineClientsCount(), 1);
        provider.removeClient(client1);
        Assert.assertTrue(provider.getOnlineClients().isEmpty());
        Assert.assertEquals(provider.getOnlineClientsCount(), 0);
    }
}
