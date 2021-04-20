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

[![LICENSE](https://img.shields.io/hexpm/l/pulsar.svg)](https://github.com/streamnative/mop/blob/master/LICENSE)

# MQTT on Pulsar (MoP)

MQTT-on-Pulsar (aka MoP) is developed to support MQTT protocol natively on Apache Pulsar.

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

    > #### Note
    > The hostname in listeners should be the same as Pulsar broker's `advertisedAddress`.

    **Example**

    ```
    mqttListeners=mqtt://127.0.0.1:1883
    advertisedAddress=127.0.0.1
    ```

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
    
    mqttProxyEnable=true
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
