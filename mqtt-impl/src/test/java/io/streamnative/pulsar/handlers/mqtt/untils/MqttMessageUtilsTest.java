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

package io.streamnative.pulsar.handlers.mqtt.untils;

import static org.mockito.Mockito.when;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Unit test for the MqttMessageUtils.
 */
public class MqttMessageUtilsTest {

    @Test
    public void testCreateClientIdentifier() {
        Channel channel = Mockito.mock(Channel.class);
        InetSocketAddress socketAddress = new InetSocketAddress("192.168.0.01", 11533);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        String clientIdentifier = MqttMessageUtils.createClientIdentifier(channel);
        Assert.assertNotNull(clientIdentifier);
        Assert.assertEquals(clientIdentifier.length(), 20);
    }

    @Test
    public void testCreateMqttConnectMessage() {
        // For MqttFixedHeader, MqttConnectVariableHeader, MqttConnectPayload are final classes
        // Mockito could not mock final class, so we have to new them.
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, false,
                MqttQoS.AT_LEAST_ONCE, true, 34);
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader("MQIsdp", 3,
                false, false, false, 0, false, true, 30);
        MqttConnectPayload payload = new MqttConnectPayload("client1", null, null,
                "".getBytes(StandardCharsets.UTF_8), null, "".getBytes(StandardCharsets.UTF_8));
        MqttConnectMessage connectMessage1 = new MqttConnectMessage(fixedHeader, variableHeader, payload);
        Channel channel = Mockito.mock(Channel.class);
        InetSocketAddress socketAddress = new InetSocketAddress("192.168.0.01", 11533);
        when(channel.remoteAddress()).thenReturn(socketAddress);
        String clientId = MqttMessageUtils.createClientIdentifier(channel);
        MqttConnectMessage connectMessage2 = MqttMessageUtils.stuffClientIdToConnectMessage(connectMessage1, clientId);
        Assert.assertEquals(connectMessage2.payload().clientIdentifier(), clientId);
        Assert.assertEquals(connectMessage2.payload().willTopic(), connectMessage1.payload().willTopic());
        Assert.assertEquals(connectMessage2.payload().willProperties(), connectMessage1.payload().willProperties());
        Assert.assertEquals(connectMessage2.payload().userName(), connectMessage1.payload().userName());
        Assert.assertEquals(connectMessage2.payload().password(), connectMessage1.payload().password());
    }
}
