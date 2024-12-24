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

import io.streamnative.pulsar.handlers.mqtt.base.MQTTTestBase;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;
import java.util.concurrent.CountDownLatch;
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
    public void testHttp() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }

}
