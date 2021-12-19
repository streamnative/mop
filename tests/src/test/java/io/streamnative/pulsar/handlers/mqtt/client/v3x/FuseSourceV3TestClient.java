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
package io.streamnative.pulsar.handlers.mqtt.client.v3x;


import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.streamnative.pulsar.handlers.mqtt.client.MqttTestClient;
import io.streamnative.pulsar.handlers.mqtt.client.options.ClientOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.ConnectOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.Optional;
import io.streamnative.pulsar.handlers.mqtt.client.options.PublishOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.SubscribeOptions;
import io.streamnative.pulsar.handlers.mqtt.client.options.UnSubscribeOptions;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

@Slf4j
public class FuseSourceV3TestClient implements MqttTestClient {

    private final MQTT mqtt;
    private BlockingConnection connection;
    private boolean autoAck;

    public FuseSourceV3TestClient(ClientOptions clientOptions) {
        this.mqtt = new MQTT();
        try {
            mqtt.setHost(clientOptions.getServerUrl(), clientOptions.getPort());
        } catch (URISyntaxException e) {
            log.error("init fuse source mqtt client fail!", e);
            throw new IllegalStateException("init fuse source mqtt client fail!");
        }
    }

    @Override
    public Optional<MqttConnAckMessage> connect() {
        return connect(ConnectOptions.builder().build());
    }

    @Override
    public Optional<MqttConnAckMessage> connect(ConnectOptions connectOptions) {
        if (connectOptions.getBasicAuth() != null) {
            mqtt.setUserName(connectOptions.getBasicAuth().getUsername());
            mqtt.setPassword(connectOptions.getBasicAuth().getPassword());
        }
        mqtt.setCleanSession(connectOptions.isCleanSession());
        this.connection = mqtt.blockingConnection();
        try {
            connection.connect();
        } catch (Exception e) {
            log.error("fuse source mqtt client connect fail ", e);
        }
        return Optional.unsupported();
    }

    @Override
    public Optional<MqttSubAckMessage> subscribe(SubscribeOptions subscribeOptions) {
        this.autoAck = subscribeOptions.isAutoAck();
        Topic[] topics = {new Topic(subscribeOptions.getTopicFilters(),
                QoS.valueOf(subscribeOptions.getQos().name()))};
        try {
            connection.subscribe(topics);
        } catch (Exception e) {
            log.error("fuse source mqtt client subscribe fail ", e);
        }
        return Optional.unsupported();
    }

    @Override
    public Optional<MqttPublishMessage> receive(long time, TimeUnit unit) {
        try {
            Message msg = connection.receive(time, unit);
            if (autoAck) {
                msg.ack();
            }
            byte[] payload = msg.getPayload();
            MqttPublishMessage publishMessage = MqttMessageBuilders.publish()
                    .qos(MqttQoS.FAILURE)
                    .payload(Unpooled.buffer().writeBytes(payload))
                    .topicName(msg.getTopic())
                    .build();
            return Optional.of(publishMessage);
        } catch (Exception e) {
            log.error("fuse source mqtt client receive fail ", e);
            return Optional.unsupported();
        }
    }

    @Override
    public Optional<MqttUnsubAckMessage> unsubscribe(UnSubscribeOptions unSubscribeOptions) {
        try {
            String[] topic = {unSubscribeOptions.getTopicFilter()};
            connection.unsubscribe(topic);
        } catch (Exception e) {
            log.error("fuse source mqtt client unsubscribe fail ", e);
        }
        return Optional.unsupported();
    }

    @Override
    public Optional<MqttPubAckMessage> publish(PublishOptions publishOptions) {
        try {
            connection.publish(publishOptions.getTopic(), publishOptions.getPayload(),
                    QoS.valueOf(publishOptions.getQos().name()), publishOptions.isRetain());
        } catch (Exception e) {
            log.error("fuse source mqtt client publish fail ", e);
        }
        return Optional.unsupported();
    }

    @Override
    public Optional<MqttMessage> disconnect() {
        try {
            connection.disconnect();
        } catch (Exception e) {
            log.error("fuse source mqtt client disconnect fail ", e);
        }
        return Optional.unsupported();
    }
}
