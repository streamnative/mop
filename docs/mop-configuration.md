# MoP available configurations

## Common

| Property | Default value | Comment
|----------| --------------| --------
|messagingProtocols | mqtt | available values [mqtt, kafka, amqp]
|protocolHandlerDirectory | ./protocols | Protocol handler directory |
|mqttListeners |  | MoP listener address. available listener prefix: [mqtt, mqtt+ssl, mqtt+ssl+psk]|
|advertisedAddress | | Keep the same as Pulsar broker's `advertisedAddress` |
|mqttAuthenticationEnabled| false | Enable mqtt authentication
|mqttAuthenticationMethods | null | Mqtt authentication methods, available values [basic, token]
|defaultTenant | public | Default Pulsar tenant that the MQTT server used |
|defaultNamespace | default | Default Pulsar namespace that the MQTT server used | 
|defaultTopicDomain | persistent | Default Pulsar topic domain that the MQTT server used |
|mqttMessageMaxLength | 8092 | Max length for per message. |


## MoP Proxy

| Property | Default value | Comment
|----------|---------------| --------
|mqttProxyEnabled | false         | Enable MoP proxy |
|mqttProxyPort | 5678          | Default MoP proxy port |
|mqttProxyTlsEnabled | false         | Enable MoP proxy TLS or not |
|mqttProxyTlsPort | 5679          | Default mqtt TLS port
|mqttProxyTlsPskPort | 5680          | Default mqtt proxy tls psk port |


## TLS

| Property                                 | Default value | Comment
|------------------------------------------| --------------| --------
| tlsEnabled                               | false | Enabled tls |
| mqttTlsCertRefreshCheckDurationSec       | 300 | Tls cert refresh duration in seconds (set 0 to check on every new connection) |
| mqttTlsCertificateFilePath               | | The path of TLS certificate path |
| mqttTlsKeyFilePath                       | null | The path of TLS key file |
| mqttTlsTrustCertsFilePath                | | Path for the trusted TLS certificate file | 
| mqttTlsProtocols                         | | TLS protocols, available values [TLSv1.3, TLSv1.2, TlSv1.1, TLSv1] |
| mqttTlsCiphers                           | | Specify the tls cipher the proxy will use to negotiate during TLS Handshake (a comma-separated list of ciphers). Examples:- [TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256] |
| mqttTlsAllowInsecureConnection           | false | Accept untrusted TLS certificate from client. If true, a client with a cert which cannot be verified with the `tlsTrustCertsFilePath`" cert will be allowed to connect to the server, though the cert will not be used for client authentication |
| mqttTlsRequireTrustedClientCertOnConnect | false | Whether client certificates are required for TLS. Connections are rejected if the client certificate isn't trusted |
| mqttTlsEnabledWithKeyStore               | false | Enable TLS with KeyStore type configuration for proxy |
| mqttTlsProvider                          | | TLS Provider |
| mqttTlsKeyStoreType                      | JKS | TLS KeyStore type configuration for proxy: JKS, PKCS12 |
| mqttTlsKeyStore                          | | TLS KeyStore path for proxy |
| mqttTlsKeyStorePassword                  | | TLS KeyStore password for proxy |
| mqttTlsTrustStoreType                    | JKS | TLS TrustStore type configuration for proxy: JKS, PKCS12 |
| mqttTlsTrustStore                        | | TLS TrustStore path for proxy | 
| mqttTlsTrustStorePassword                | | TLS TrustStore password for proxy |


## TLS-PSK

| Property               | Default value | Comment
|------------------------| --------------| --------
| mqttTlsPskIdentityFile | | When you want identities in a single file with many pairs, you can config this. Identities will load from both `tlsPskIdentity` and `tlsPskIdentityFile` |
| mqttTlsPskEnabled      | false | Enable tls psk |
| mqttTlsPskIdentityHint |  | Any string can be specified |
| mqttTlsPskIdentity     |  | Identity is semicolon list of string with identity:secret format |


## Socket

| Property | Default value | Comment
|----------| --------------| --------
|maxNoOfChannels | 64 | The maximum number of channels which can exist concurrently on a connection |
|maxFrameSize | 4 * 1024 * 1024 | The maximum frame size on a connection |
|mqttProxyNumAcceptorThreads | 1 | Number of threads to use for Netty Acceptor. Default is set to `1` |
|mqttProxyNumIOThreads | Runtime.getRuntime().availableProcessors() | Number of threads to use for Netty IO |


