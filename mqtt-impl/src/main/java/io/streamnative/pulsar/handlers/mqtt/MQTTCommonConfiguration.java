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
package io.streamnative.pulsar.handlers.mqtt;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * MQTT on Pulsar service common configuration object.
 */
@Getter
@Setter
public class MQTTCommonConfiguration extends ServiceConfiguration {

    @Category
    public static final String CATEGORY_MQTT = "MQTT on Pulsar";
    @Category
    public static final String CATEGORY_MQTT_PROXY = "MQTT Proxy";
    @Category
    public static final String CATEGORY_TLS = "TLS";
    @Category
    public static final String CATEGORY_TLS_PSK = "TLS-PSK";
    @Category
    public static final String CATEGORY_KEYSTORE_TLS = "KeyStoreTLS";

    @FieldContext(
            category = CATEGORY_MQTT,
            required = false,
            doc = "Whether enable authentication for MQTT."
    )
    private boolean mqttAuthenticationEnabled = false;

    @FieldContext(
            category = CATEGORY_MQTT,
            doc = "A comma-separated list of authentication methods to enable."
    )
    private List<String> mqttAuthenticationMethods = ImmutableList.of();

    @FieldContext(
            category = CATEGORY_MQTT,
            required = false,
            doc = "Whether enable authorization for MQTT."
    )
    private boolean mqttAuthorizationEnabled = false;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "The maximum number of channels which can exist concurrently on a connection."
    )
    private int maxNoOfChannels = 64;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "The maximum frame size on a connection."
    )
    private int maxFrameSize = 4 * 1024 * 1024;

    @FieldContext(
            category = CATEGORY_MQTT,
            required = false,
            doc = "Server uses this value to limit the number of QoS 1 and QoS 2 publications that it is willing to"
                    + "process concurrently for the Client. It does not provide a mechanism to limit the QoS 0"
                    + " publications that the Client might try to send."
    )
    private int receiveMaximum = 65535;
    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "Default Pulsar tenant that the MQTT server used."
    )
    private String defaultTenant = "public";

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "Default Pulsar namespace that the MQTT server used."
    )
    private String defaultNamespace = "default";

    @FieldContext(
            category = CATEGORY_MQTT,
            required = true,
            doc = "Default Pulsar topic domain that the MQTT server used."
    )
    private String defaultTopicDomain = "persistent";

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "The mqtt proxy port"
    )
    private int mqttProxyPort = 5682;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "The mqtt proxy tls port"
    )
    private int mqttProxyTlsPort = 5683;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "The mqtt proxy tls psk port"
    )
    private int mqttProxyTlsPskPort = 5684;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "Deprecated, use `mqttProxyEnabled` instead"
    )
    @Deprecated
    private boolean mqttProxyEnable = false;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "Whether start mqtt protocol handler with proxy"
    )
    private boolean mqttProxyEnabled = false;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "Whether start mqtt protocol handler with proxy tls"
    )
    private boolean mqttProxyTlsEnabled = false;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "Whether start mqtt protocol handler with proxy tls psk"
    )
    private boolean mqttProxyTlsPskEnabled = false;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            required = false,
            doc = "Number of threads to use for Netty Acceptor. Default is set to `1`"
    )
    private int mqttProxyNumAcceptorThreads = 1;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            doc = "Number of threads to use for Netty IO."
                    + " Default is set to `Runtime.getRuntime().availableProcessors()`"
    )
    private int mqttProxyNumIOThreads = Runtime.getRuntime().availableProcessors();

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            doc = "Maximum number of lookup requests allowed on "
                    + "each broker connection to prevent overloading a broker."
    )
    private int mqttProxyClientMaxLookupRequest = 50000;

    @FieldContext(
            category = CATEGORY_MQTT_PROXY,
            doc = "The number of concurrent lookup requests that can be sent on each broker connection. "
                    + "Setting a maximum prevents overloading a broker."
    )
    private int mqttProxyClientConcurrentLookupRequest = 5000;

    @FieldContext(
            category = CATEGORY_TLS,
            required = false,
            doc = "Whether broker start mqtt protocol handler with tls psk"
    )
    private boolean tlsPskEnabled = false;

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Tls cert refresh duration in seconds (set 0 to check on every new connection)"
    )
    private long tlsCertRefreshCheckDurationSec = 300; // 5 mins

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Path for the TLS certificate file"
    )
    private String tlsCertificateFilePath;

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Path for the TLS private key file"
    )
    private String tlsKeyFilePath;

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Path for the trusted TLS certificate file.\n\n"
                    + "This cert is used to verify that any certs presented by connecting clients"
                    + " are signed by a certificate authority. If this verification fails, then the"
                    + " certs are untrusted and the connections are dropped"
    )
    private String tlsTrustCertsFilePath;

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Specify the tls protocols the broker will use to negotiate during TLS handshake"
                    + " (a comma-separated list of protocol names).\n\n"
                    + "Examples:- [TLSv1.3, TLSv1.2]"
    )
    private Set<String> tlsProtocols = Sets.newTreeSet();
    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Specify the tls cipher the proxy will use to negotiate during TLS Handshake"
                    + " (a comma-separated list of ciphers).\n\n"
                    + "Examples:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256]"
    )
    private Set<String> tlsCiphers = Sets.newTreeSet();

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Accept untrusted TLS certificate from client.\n\n"
                    + "If true, a client with a cert which cannot be verified with the `tlsTrustCertsFilePath`"
                    + " cert will be allowed to connect to the server, though the cert will not be used for"
                    + " client authentication"
    )
    private boolean tlsAllowInsecureConnection = false;

    @FieldContext(
            category = CATEGORY_TLS,
            doc = "Whether client certificates are required for TLS.\n\n"
                    + " Connections are rejected if the client certificate isn't trusted"
    )
    private boolean tlsRequireTrustedClientCertOnConnect = false;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "Enable TLS with KeyStore type configuration for proxy"
    )
    private boolean tlsEnabledWithKeyStore = false;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS Provider"
    )
    private String tlsProvider = null;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS KeyStore type configuration for proxy: JKS, PKCS12"
    )
    private String tlsKeyStoreType = "JKS";

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS KeyStore path for proxy"
    )
    private String tlsKeyStore = null;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS KeyStore password for proxy"
    )
    private String tlsKeyStorePassword = null;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore type configuration for proxy: JKS, PKCS12"
    )
    private String tlsTrustStoreType = "JKS";

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore path for proxy"
    )
    private String tlsTrustStore = null;

    @FieldContext(
            category = CATEGORY_KEYSTORE_TLS,
            doc = "TLS TrustStore password for proxy"
    )
    private String tlsTrustStorePassword = null;

    @FieldContext(
            category = CATEGORY_TLS_PSK,
            doc = "TLS psk identity hint"
    )
    private String tlsPskIdentityHint = null;

    @FieldContext(
            category = CATEGORY_TLS_PSK,
            doc = "TLS psk identity file"
    )
    private String tlsPskIdentityFile = null;

    @FieldContext(
            category = CATEGORY_TLS_PSK,
            doc = "TLS psk identity with plain text"
    )
    private String tlsPskIdentity = null;

    @FieldContext(
            category = CATEGORY_MQTT,
            doc = "Max length for per message."
    )
    private int mqttMessageMaxLength = 8092;
}
