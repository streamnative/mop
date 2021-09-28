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

import io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for the ConfigurationUtils.
 */
public class ConfigurationUtilsTest {

    @Test
    public void testGetListenerPort() {
        String plainTextListener = "mqtt://127.0.0.1:1883";
        Assert.assertEquals(ConfigurationUtils.getListenerPort(plainTextListener), 1883);
        String sslListener = "mqtt+ssl://127.0.0.1:8883";
        Assert.assertEquals(ConfigurationUtils.getListenerPort(sslListener), 8883);
        String sslPskListener = "mqtt+ssl+psk://127.0.0.1:8884";
        Assert.assertEquals(ConfigurationUtils.getListenerPort(sslPskListener), 8884);
        try {
            String sslInvalidListener = "mqtt+ssl+://127.0.0.1:8883";
            ConfigurationUtils.getListenerPort(sslInvalidListener);
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals(ex.getMessage(), "listener not match pattern");
        }
        try {
            String sslPskInvalidListener = "mqtt+ssl+psk+://127.0.0.1:8884";
            ConfigurationUtils.getListenerPort(sslPskInvalidListener);
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals(ex.getMessage(), "listener not match pattern");
        }
    }
}
