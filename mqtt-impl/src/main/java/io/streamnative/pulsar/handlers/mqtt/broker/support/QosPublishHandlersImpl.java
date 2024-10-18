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
package io.streamnative.pulsar.handlers.mqtt.broker.support;

/**
 * Default implementation of QosPublishHandlers.
 */
public class QosPublishHandlersImpl implements QosPublishHandlers {

    private final QosPublishHandler qos0Handler;
    private final QosPublishHandler qos1Handler;
    private final QosPublishHandler qos2Handler;

    public QosPublishHandlersImpl(MQTTService mqttService) {
        this.qos0Handler = new Qos0PublishHandler(mqttService);
        this.qos1Handler = new Qos1PublishHandler(mqttService);
        this.qos2Handler = new Qos2PublishHandler(mqttService);
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
