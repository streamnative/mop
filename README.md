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

[![LICENSE](https://img.shields.io/hexpm/l/pulsar.svg)](https://github.com/streamnative/aop/blob/master/LICENSE)


# MQTT on Pulsar (MoP)
MQTT-on-Pulsar (aka MoP) was developed to support MQTT protocol natively on Apache Pulsar.

## Get started
### Download or build MoP protocol handler
1. clone this project from GitHub to your local.

```bash
git clone https://github.com/streamnative/mop.git
cd mop
```

2. build the project.
```bash
mvn clean install -DskipTests
```

3. the nar file can be found at this location.
```bash
./mqtt-impl/target/pulsar-protocol-handler-mqtt-${version}.nar
```

### Install MoP protocol handler
All what you need to do is to configure the Pulsar broker to run the Mop protocol handler as a plugin, that is,
add configurations in Pulsar's configuration file, such as `broker.conf` or `standalone.conf`.

1. Set the configuration of the MoP protocol handler.
    
    Add the following properties and set their values in Pulsar configuration file, such as `conf/broker.conf` or `conf/standalone.conf`.

    Property | Set it to the following value | Default value
    |---|---|---
    `messagingProtocols` | mqtt | null
    `protocolHandlerDirectory`| Location of MoP NAR file | ./protocols
    
    **Example**

    ```
    messagingProtocols=mqtt
    protocolHandlerDirectory=./protocols
    ```
2. Set MQTT server listeners.

    > #### Note
    > The hostname in listeners should be the same as Pulsar broker's `advertisedAddress`.

    **Example**

    ```
    mqttListeners=mqtt://127.0.0.1:1883
    advertisedAddress=127.0.0.1
    ```
   
### Restart Pulsar brokers to load MoP

After you have installed the MoP protocol handler to Pulsar broker, you can restart the Pulsar brokers to load MoP.

### Verify MoP

There are many MQTT client can be used to verify MoP such as http://workswithweb.com/mqttbox.html, https://www.hivemq.com/mqtt-toolbox. You can choose a cli tool or interface tool to verify the MoP.

#### Verify with fusesource mqtt-client

```java
<dependency>
    <groupId>org.fusesource.mqtt-client</groupId>
    <artifactId>mqtt-client</artifactId>
    <version>1.16</version>
</dependency>
```

Publish messages and consume messages:

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
