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

package io.streamnative.pulsar.handlers.mqtt.mqtt3.fusesource.proxy;

import static org.awaitility.Awaitility.await;
import com.google.gson.Gson;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration tests for MQTT protocol handler with proxy.
 */
@Slf4j
public class ProxyHttpTest extends MQTTTestBase {

    @Override
    protected MQTTCommonConfiguration initConfig() throws Exception {
        MQTTCommonConfiguration mqtt = super.initConfig();
        mqtt.setMqttProxyEnabled(true);
        return mqtt;
    }

    @Test
    public void testGetDeviceList() throws Exception {
        List<Integer> mqttProxyPortList = getMqttProxyPortList();
        List<Integer> mqttProxyHttpPortList = getMqttProxyHttpPortList();
        int index = random.nextInt(mqttProxyPortList.size());
        MQTT mqttProducer = new MQTT();
        int port = mqttProxyPortList.get(index);
        String clientId = "device-list-client";
        mqttProducer.setHost("127.0.0.1", port);
        mqttProducer.setClientId(clientId);
        BlockingConnection producer = mqttProducer.blockingConnection();
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            producer.connect();
            producer.publish("testHttp", "Hello MQTT".getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
            final String mopEndPoint = "http://localhost:" + mqttProxyHttpPortList.get(index) + "/admin/devices/list";
            await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).untilAsserted(() -> {
                String ret = getDeviceList(httpClient, mopEndPoint);
                ArrayList<?> deviceList = new Gson().fromJson(ret, ArrayList.class);
                Assert.assertNotNull(deviceList, "Invalid device list response: " + ret);
                Assert.assertEquals(deviceList.size(), 1, "Unexpected device list response: " + ret);
                Assert.assertTrue(deviceList.contains(clientId), "Unexpected device list response: " + ret);
            });
        } finally {
            producer.disconnect();
        }
    }

    private String getDeviceList(CloseableHttpClient httpClient, String mopEndPoint) throws Exception {
        HttpResponse response = httpClient.execute(new HttpGet(mopEndPoint));
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200,
                "Unexpected HTTP response from " + mopEndPoint + ": " + body);
        return body;
    }
}
