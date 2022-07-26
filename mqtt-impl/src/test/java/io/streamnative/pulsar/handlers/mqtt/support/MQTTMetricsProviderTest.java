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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * MQTT metrics provider test.
 */
public class MQTTMetricsProviderTest {

    private MQTTServerConfiguration serverConfiguration;
    private MQTTMetricsCollector metricsCollector;
    private SimpleTextOutputStream outputStream;
    private ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer();

    @BeforeMethod
    public void setUp() {
        outputStream = new SimpleTextOutputStream(buf);
        serverConfiguration = mock(MQTTServerConfiguration.class);
        metricsCollector = new MQTTMetricsCollector(serverConfiguration);
        doReturn("mop-cluster").when(serverConfiguration).getClusterName();
    }

    @Test
    public void testGenerate() {
        MQTTMetricsProvider provider = new MQTTMetricsProvider(metricsCollector);
        metricsCollector.addClient("192.168.1.0:11022");
        provider.generate(outputStream);
        String result = new String(buf.array(), buf.arrayOffset(), buf.readableBytes());
        buf.release();
        Assert.assertTrue(result.contains("mop_active_client_count"));
        Assert.assertTrue(result.contains("mop_total_client_count"));
        Assert.assertTrue(result.contains("mop_maximum_client_count"));
        Assert.assertTrue(result.contains("mop_sub_count"));
        Assert.assertTrue(result.contains("mop_send_count"));
        Assert.assertTrue(result.contains("mop_send_bytes"));
        Assert.assertTrue(result.contains("mop_received_count"));
        Assert.assertTrue(result.contains("mop_received_bytes"));
    }
}
