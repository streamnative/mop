<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/37f8e4d44ffd468c9255fbbb231b261c)](https://app.codacy.com/gh/streamnative/mop?utm_source=github.com&utm_medium=referral&utm_content=streamnative/mop&utm_campaign=Badge_Grade_Settings)
[![LICENSE](https://img.shields.io/hexpm/l/pulsar.svg)](https://github.com/streamnative/mop/blob/master/LICENSE)

# MQTT on Pulsar (MoP)

MQTT-on-Pulsar (aka MoP) is developed to support MQTT protocol natively on Apache Pulsar. Currently, only MQTT 3.1.1 supported.

## Get started

### Download or build MoP protocol handler

1. Clone the MoP project from GitHub to your local.

    ```bash
    git clone https://github.com/streamnative/mop.git
    cd mop
    ```

2. Build the project.

    ```bash
    mvn clean install -DskipTests
    ```

3. The NAR file can be found at this location.

    ```bash
    ./mqtt-impl/target/pulsar-protocol-handler-mqtt-${version}.nar
    ```

### Install MoP protocol handler

Configure the Pulsar broker to run the MoP protocol handler as a plugin by adding configurations to the Pulsar configuration file, such as `broker.conf` or `standalone.conf`.

1. Set the configuration of the MoP protocol handler.
    
    Add the following properties and set their values in the Pulsar configuration file, such as `conf/broker.conf` or `conf/standalone.conf`.

    | Property | Suggested value | Default value |
    |---|---|---
    `messagingProtocols` | mqtt | null
    `protocolHandlerDirectory`| Location of MoP NAR file | ./protocols
    
    **Example**

    ```
    messagingProtocols=mqtt
    protocolHandlerDirectory=./protocols
    ```
2. Set the MQTT server listeners.

    **Example**

    ```
    mqttListeners=mqtt://127.0.0.1:1883
    advertisedAddress=127.0.0.1
    ```

   > #### Note
   > The default hostname of `advertisedAddress` is InetAddress.getLocalHost().getHostName(). 
   > If you'd like to config this, please keep the same as Pulsar broker's `advertisedAddress`.

### Load MoP protocol handler

After you install the MoP protocol handler to Pulsar broker, you can restart the Pulsar brokers to load the MoP protocol handler.

### How to use Proxy

To use the proxy, follow the following steps. For detailed steps, refer to [Deploy a cluster on bare metal](http://pulsar.apache.org/docs/en/deploy-bare-metal/).

1. Prepare a ZooKeeper cluster.

2. Initialize the cluster metadata.

3. Prepare a BookKeeper cluster.

4. Copy the `pulsar-protocol-handler-mqtt-${version}.nar` to the `$PULSAR_HOME/protocols` directory.

5. Start the Pulsar broker.

   Here is an example of the Pulsar broker configuration.

    ```yaml
    messagingProtocols=mqtt
    protocolHandlerDirectory=./protocols
    brokerServicePort=6651
    mqttListeners=mqtt://127.0.0.1:1883
    advertisedAddress=127.0.0.1
    
    mqttProxyEnabled=true
    mqttProxyPort=5682
    ```

### Verify MoP protocol handler

There are many MQTT client that can be used to verify the MoP protocol handler, such as [MQTTBox](http://workswithweb.com/mqttbox.html), [MQTT Toolbox](https://www.hivemq.com/mqtt-toolbox). You can choose a CLI tool or interface tool to verify the MoP protocol handler.

The following example shows how to verify the MoP protocol handler with FuseSource MqttClient.

1. Add the dependency.

    ```java
    <dependency>
        <groupId>org.fusesource.mqtt-client</groupId>
        <artifactId>mqtt-client</artifactId>
        <version>1.16</version>
    </dependency>
    ```

2. Publish messages and consume messages.

    ```java
    MQTT mqtt = new MQTT();
    mqtt.setHost("127.0.0.1", 1883);
    BlockingConnection connection = mqtt.blockingConnection();
    connection.connect();
    Topic[] topics = { new Topic("persistent://public/default/my-topic", QoS.AT_LEAST_ONCE) };
    connection.subscribe(topics);

    // publish message
    connection.publish("persistent://public/default/my-topic", "Hello MOP!".getBytes(), QoS.AT_LEAST_ONCE, false);

    // receive message
    Message received = connection.receive();
    ```
## Security

### Enabling Authentication

MoP currently supports `basic` and `token` authentication methods. The `token` authentication method works
with any of the token based Pulsar authentication providers such as the built-in JWT provider and external token
authentication providers like [biscuit-pulsar](https://github.com/CleverCloud/biscuit-pulsar).

To use authentication for MQTT connections your Pulsar cluster must already have authentication enabled with your
chosen authentication provider(s) configured.

You can then enable MQTT configuration with the following configuration properties:
```yaml
mqttAuthenticationEnabled=true
mqttAuthenticationMethods=token
```

`mqttAuthenticationMethods` can be set to a comma delimited list if you wish to enable multiple authentication providers.
MoP will attempt each in order when authenticating client connections.

With authentication enabled MoP will not allow anonymous connections currently.

#### Authenticating client connections

##### Basic Authentication
Set the MQTT username and password client settings.

##### Token Authentication
Set the MQTT password to the token body, currently username will be disregarded but MUST be set to some value as this is required by the MQTT specification.


### Enabling TLS

MoP currently supports TLS transport encryption.

Generate crt and key file :
```
openssl genrsa 2048 > server.key
chmod 400 server.key
openssl req -new -x509 -nodes -sha256 -days 365 -key server.key -out server.crt
```

#### TLS with broker

1. Config mqtt broker to load tls config.
```conf
...
tlsEnabled=true
mqttListeners=mqtt+ssl://127.0.0.1:8883
tlsCertificateFilePath=/xxx/server.crt
tlsKeyFilePath=/xxx/server.key
...
```

2. Config client to load tls config.
```java
MQTT mqtt = new MQTT();
// default tls port
mqtt.setHost(URI.create("ssl://127.0.0.1:8883")); 
File crtFile = new File("server.crt");
Certificate certificate = CertificateFactory.getInstance("X.509").generateCertificate(new FileInputStream(crtFile));
KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
keyStore.load(null, null);
keyStore.setCertificateEntry("server", certificate);
TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
trustManagerFactory.init(keyStore);
SSLContext sslContext = SSLContext.getInstance("TLS");
sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
mqtt.setSslContext(sslContext);
BlockingConnection connection = mqtt.blockingConnection();
connection.connect();
...
```


#### TLS with proxy

1. Config mqtt broker to load tls config.
```conf
...
mqttProxyEnable=true
mqttProxyTlsEnabled=true
tlsCertificateFilePath=/xxx/server.crt
tlsKeyFilePath=/xxx/server.key
...
```

2. Config client to load tls config.
```java
MQTT mqtt = new MQTT();
// default proxy tls port
mqtt.setHost(URI.create("ssl://127.0.0.1:5683")); 
File crtFile = new File("server.crt");
Certificate certificate = CertificateFactory.getInstance("X.509").generateCertificate(new FileInputStream(crtFile));
KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
keyStore.load(null, null);
keyStore.setCertificateEntry("server", certificate);
TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
trustManagerFactory.init(keyStore);
SSLContext sslContext = SSLContext.getInstance("TLS");
sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
mqtt.setSslContext(sslContext);
BlockingConnection connection = mqtt.blockingConnection();
connection.connect();
...
```

#### TLS PSK with broker

Please reference [here](https://en.wikipedia.org/wiki/TLS-PSK) to learn more about TLS-PSK.

1. Config mqtt broker to load tls psk config.
```conf
...
tlsPskEnabled=true
mqttListeners=mqtt+ssl+psk://127.0.0.1:8884
// any string can be specified
tlsPskIdentityHint=alpha
// identity is semicolon list of string with identity:secret format
tlsPskIdentity=mqtt:mqtt123
...
```
Optional configs

|  Config key   | Comment  |
|  :---------:  | -------- |
|  tlsPskIdentityFile | When you want identities in a single file with many pairs, you can config this. Identities will load from both `tlsPskIdentity` and `tlsPskIdentityFile`  |
|  tlsProtocols  | TLS PSK protocols, default are [ TLSv1, TLSv1.1, TLSv1.2 ]  |
|  tlsCiphers | TLS PSK ciphers, default are [ TLS_ECDHE_PSK_WITH_CHACHA20_POLY1305_SHA256, TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA, TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA, TLS_PSK_WITH_AES_128_CBC_SHA, TLS_PSK_WITH_AES_256_CBC_SHA ] |

2. As current known mqtt Java client does not support TLS-PSK, it's better to verify this by `mosquitto cli`
```cli
# Default with tlsv1.2
mosquitto_pub --psk-identity mqtt --psk 6d717474313233 -p 8884 -t "/a/b/c" -m "hello mqtt"

# Test with tlsv1.1
mosquitto_pub --psk-identity mqtt --psk 6d717474313233 -p 8884 -t "/a/b/c" -m "hello mqtt" --tls-version tlsv1.1
```
- Download [mosquitto](https://mosquitto.org/download/) with Mac version.
- The secret `mqtt123` is converted to `6d717474313233` using [Hex Code Converter](https://www.rapidtables.com/convert/number/ascii-to-hex.html)

#### TLS PSK with proxy

1. Config mqtt proxy to load tls psk config.
```conf
...
mqttProxyEnable=true
mqttProxyTlsPskEnabled=true
// default tls psk port
mqttProxyTlsPskPort=5684
// any string can be specified
tlsPskIdentityHint=alpha
// identity is semicolon list of string with identity:secret format
tlsPskIdentity=mqtt:mqtt123
...
```

2. Test with `mosquitto cli`
```
mosquitto_pub --psk-identity mqtt --psk 6d717474313233 -p 5684 -t "/a/b/c" -m "hello mqtt"
```

## Topic Names & Filters

For Apache Pulsar, The topic name consists of 4 parts:

```
<domain>://<tenant>/<namespace>/<local-name>
```
And `/` is not allowed in the local topic name. But for the MQTT topic name can have multiple levels such as:

```
/a/b/c/d/e/f
```

MoP mapping the MQTT topic name to Pulsar topic name as follows:

1. If the MQTT topic name does not start with the topic domain, MoP treats the URL encoded MQTT topic name as the Pulsar local topic name, and the default tenant and default namespace will be used to map the Pulsar topic name.
2. If the MQTT topic name starts with the topic domain, MoP will treat the first level topic name as the tenant and the second level topic name as the namespace and the remaining topic name levels will be covert as the local topic name with URL encoded.

Examples:

|  MQTT topic name   | Apache Pulsar topic name  |
|  ----  | ----  |
| /a/b/c  | persistent://public/default/%2Fa%2Fb%2Fc |
| a  | persistent://public/default/a |
| persistent://my-tenant/my-ns/a/b/c  | persistent://my-tenant/my-ns/a%2Fb%2Fc |
| persistent://my-tenant/my-ns/a  | persistent://my-tenant/my-ns/a |
| non-persistent://my-tenant/my-ns/a  | non-persistent://my-tenant/my-ns/a |
| non-persistent://my-tenant/my-ns/a/b/c  | non-persistent://my-tenant/my-ns/a%2Fb%2Fc |

So if you want to consume messages by Pulsar Client from the topic `/a/b/c`, the topic name for the Pulsar consumer should be `persistent://public/default/%2Fa%2Fb%2Fc`. If you want to consume messages from a Pulsar topic by the MQTT client, use the Pulsar topic name as the MQTT topic name directly.

MoP topic supports single-level wildcard `+` and multi-level wildcard `#`. The topic name filter also follows the above topic name mapping rules.

1. If the topic filter starts with the topic domain, MoP only filters the topic under the namespace that the topic filter provided.
2. If the topic filter does not start with the topic domain, MoP only filters the topic name under the default namespace.

Examples:

|  MQTT topic name   | Topic filter  | Is match |
|  ----  | ----  | ----  |
| /a/b/c  | /a/+/c  | Yes |
| /a/b/c  | /a/#  | Yes |
| /a/b/c  | a/#  | No |
| /a/b/c  | persistent://my-tenant/my-namespace//a/#  | No |
| /a/b/c  | persistent://public/default//a/#  | Yes |
| persistent://public/default/a/b/c  | persistent://public/default/a/#  | Yes |
| persistent://public/default/a/b/c  | persistent://public/default/a/+/c  | Yes |
| persistent://public/default/a/b/c  | persistent://public/default//a/+/c  | No |
| persistent://public/default/a/b/c  | persistent://my-tenant/my-namespace/a/+/c  | No |


> Notice:
>
> The default tenant and the default namespace for the MoP are configurable, by default, the default tenant is `public` and the default namespace is `default`.

## MoP available configurations

Please refer [here](docs/mop-configuration.md)

## Project maintainers

-   [@Technoboy-](https://github.com/Technoboy-)
-   [@codelipenghui](https://github.com/codelipenghui)
