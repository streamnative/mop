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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonConfiguration;
import java.util.Set;
import org.testng.annotations.Test;

public class ConfigurationCompatibilityTest {

    @Test
    public void testTlsConfigurationCompatibility() {
        final MQTTCommonConfiguration conf = new MQTTCommonConfiguration();
        final String identity = "test";
        conf.setTlsPskIdentity(identity);
        assertEquals(conf.getMqttTlsPskIdentity(), identity);
        final String testFile = "testFile";
        conf.setTlsPskIdentityFile(testFile);
        assertEquals(conf.getMqttTlsPskIdentityFile(), testFile);
        final String testHint = "hint";
        conf.setTlsPskIdentityHint(testHint);
        assertEquals(conf.getMqttTlsPskIdentityHint(), testHint);
        conf.setTlsPskEnabled(true);
        assertTrue(conf.isMqttTlsPskEnabled());
        final String psw = "psw";
        conf.setTlsTrustStorePassword(psw);
        assertEquals(conf.getMqttTlsTrustStorePassword(), psw);
        final String store = "store";
        conf.setTlsTrustStore(store);
        assertEquals(conf.getMqttTlsTrustStore(), store);
        final String type = "ABC";
        conf.setTlsTrustStoreType(type);
        assertEquals(conf.getMqttTlsTrustStoreType(), type);
        final String ksPsw = "ksPsw";
        conf.setTlsKeyStorePassword(ksPsw);
        assertEquals(conf.getMqttTlsKeyStorePassword(), ksPsw);
        final String kStore = "kstore";
        conf.setTlsKeyStore(kStore);
        assertEquals(conf.getMqttTlsKeyStore(), kStore);
        final String ksType = "ksType";
        conf.setTlsKeyStoreType(ksType);
        assertEquals(conf.getMqttTlsKeyStoreType(), ksType);
        final String provider = "provider";
        conf.setTlsProvider(provider);
        assertEquals(conf.getMqttTlsProvider(), provider);
        conf.setTlsEnabledWithKeyStore(true);
        assertTrue(conf.isMqttTlsEnabledWithKeyStore());
        conf.setTlsRequireTrustedClientCertOnConnect(true);
        assertTrue(conf.isMqttTlsRequireTrustedClientCertOnConnect());
        conf.setTlsAllowInsecureConnection(true);
        assertTrue(conf.isMqttTlsAllowInsecureConnection());
        final Set<String> cipher = Sets.newHashSet("cipher");
        conf.setTlsCiphers(cipher);
        assertEquals(conf.getMqttTlsCiphers(), cipher);
        final Set<String> protocols = Sets.newHashSet("123");
        conf.setTlsProtocols(protocols);
        assertEquals(conf.getMqttTlsProtocols(), protocols);
        final String path = "path";
        conf.setTlsTrustCertsFilePath(path);
        assertEquals(conf.getMqttTlsTrustCertsFilePath(), path);
        final String tlsKeyPath = "tlsKeyPath";
        conf.setTlsKeyFilePath(tlsKeyPath);
        assertEquals(conf.getMqttTlsKeyFilePath(), tlsKeyPath);
        final String tlsCertPath = "tlsCertPath";
        conf.setTlsCertificateFilePath(tlsCertPath);
        assertEquals(conf.getMqttTlsCertificateFilePath(), tlsCertPath);
        final long durationSec = 214314;
        conf.setTlsCertRefreshCheckDurationSec(durationSec);
        assertEquals(conf.getMqttTlsCertRefreshCheckDurationSec(), durationSec);
    }

}
