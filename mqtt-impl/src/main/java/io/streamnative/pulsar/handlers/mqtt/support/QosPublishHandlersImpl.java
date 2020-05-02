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

import io.streamnative.pulsar.handlers.mqtt.ConnectionDescriptorStore;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandler;
import io.streamnative.pulsar.handlers.mqtt.QosPublishHandlers;
import org.apache.pulsar.broker.PulsarService;

/**
 * Default implementation of QosPublishHandlers.
 */
public class QosPublishHandlersImpl implements QosPublishHandlers {

    private final PulsarService pulsarService;
    private final MQTTServerConfiguration configuration;
    private final QosPublishHandler qos0Handler;
    private final QosPublishHandler qos1Handler;
    private final QosPublishHandler qos2Handler;

    public QosPublishHandlersImpl(PulsarService pulsarService, MQTTServerConfiguration configuration) {
        this.pulsarService = pulsarService;
        this.configuration = configuration;
        this.qos0Handler = new Qos0PublishHandler(pulsarService, configuration,
                ConnectionDescriptorStore.getInstance());
        this.qos1Handler = new Qos1PublishHandler(pulsarService, configuration,
                ConnectionDescriptorStore.getInstance());
        this.qos2Handler = new Qos2PublishHandler(pulsarService, configuration,
                ConnectionDescriptorStore.getInstance());
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
