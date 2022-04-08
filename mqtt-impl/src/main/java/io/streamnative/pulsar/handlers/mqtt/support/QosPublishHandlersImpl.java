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
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandler;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandlers;

/**
 * Default implementation of QosPublishHandlers.
 */
public class QosPublishHandlersImpl implements QosPublishHandlers {

    private final QosPublishHandler qos0Handler;
    private final QosPublishHandler qos1Handler;
    private final QosPublishHandler qos2Handler;

    public QosPublishHandlersImpl(MQTTService mqttService, MQTTServerConfiguration configuration, Channel channel) {
        this.qos0Handler = new Qos0PublishHandler(mqttService, configuration, channel);
        this.qos1Handler = new Qos1PublishHandler(mqttService, configuration, channel);
        this.qos2Handler = new Qos2PublishHandler(mqttService, configuration, channel);
    }

    @Override
    public QosPublishHandler qos0() {
        return this.qos0Handler;
    }

    @Override
    public QosPublishHandler qos1() {
        return this.qos1Handler;
    }

    @Override
    public QosPublishHandler qos2() {
        return this.qos2Handler;
    }
}
