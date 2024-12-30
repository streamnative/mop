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

import com.google.gson.Gson;
import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import lombok.extern.slf4j.Slf4j;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
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
        int index = random.nextInt(mqttProxyPortList.size());
        List<Integer> mqttProxyPortList = getMqttProxyPortList();
        List<Integer> mqttProxyHttpPortList = getMqttProxyHttpPortList();
        MQTT mqttProducer = new MQTT();
        int port = mqttProxyPortList.get(index);
        String clientId = "device-list-client";
        mqttProducer.setHost("127.0.0.1", port);
        mqttProducer.setClientId(clientId);
        BlockingConnection producer = mqttProducer.blockingConnection();
        producer.connect();
        producer.publish("testHttp", "Hello MQTT".getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE, false);
        Thread.sleep(4000);
        HttpClient httpClient = HttpClientBuilder.create().build();
        final String mopEndPoint = "http://localhost:" + mqttProxyHttpPortList.get(index) + "/admin/devices/list";
        HttpResponse response = httpClient.execute(new HttpGet(mopEndPoint));
        InputStream inputStream = response.getEntity().getContent();
        InputStreamReader isReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isReader);
        StringBuffer buffer = new StringBuffer();
        String str;
        while ((str = reader.readLine()) != null){
            buffer.append(str);
        }
        String ret = buffer.toString();
        ArrayList deviceList = new Gson().fromJson(ret, ArrayList.class);
        Assert.assertEquals(deviceList.size(), 1);
        Assert.assertTrue(deviceList.contains(clientId));
    }

}
