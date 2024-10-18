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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import io.streamnative.pulsar.handlers.mqtt.broker.metric.MQTTMetricsCollector;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Mqtt metrics collector test.
 */
public class MQTTMetricsCollectorTest {

    private MQTTServerConfiguration serverConfiguration;

    @BeforeMethod
    public void setUp() {
        serverConfiguration = mock(MQTTServerConfiguration.class);
        doReturn("mop-cluster").when(serverConfiguration).getClusterName();
    }

    @Test
    public void testData() {
        MQTTMetricsCollector metricsCollector = new MQTTMetricsCollector(serverConfiguration);
        String client1 = "192.168.1.0:11021";
        metricsCollector.addClient(client1);
        Assert.assertTrue(metricsCollector.getClientMetrics().getActiveClients().contains(client1));
        Assert.assertEquals(metricsCollector.getClientMetrics().getActiveCount(), 1);
        Assert.assertEquals(metricsCollector.getClientMetrics().getTotalCount(), 1);
        Assert.assertEquals(metricsCollector.getClientMetrics().getMaximumCount(), 1);
        metricsCollector.addClient(client1);
        Assert.assertEquals(metricsCollector.getClientMetrics().getActiveCount(), 1);
        String client2 = "192.168.1.0:11022";
        metricsCollector.addClient(client2);
        Assert.assertEquals(metricsCollector.getClientMetrics().getActiveCount(), 2);
        Assert.assertEquals(metricsCollector.getClientMetrics().getTotalCount(), 2);
        Assert.assertEquals(metricsCollector.getClientMetrics().getMaximumCount(), 2);
        metricsCollector.removeClient(client1);
        Assert.assertEquals(metricsCollector.getClientMetrics().getActiveCount(), 1);
        Assert.assertEquals(metricsCollector.getClientMetrics().getMaximumCount(), 2);
        Assert.assertEquals(metricsCollector.getClientMetrics().getTotalCount(), 2);
    }
}
