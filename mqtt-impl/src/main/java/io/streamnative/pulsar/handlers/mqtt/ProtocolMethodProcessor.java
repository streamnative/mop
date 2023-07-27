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

import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;

/**
 * Interface for MQTT protocol method processor.
 */
public interface ProtocolMethodProcessor {

    void processConnect(MqttAdapterMessage msg);

    void processPubAck(MqttAdapterMessage msg);

    void processPublish(MqttAdapterMessage msg);

    void processPubRel(MqttAdapterMessage msg);

    void processPubRec(MqttAdapterMessage msg);

    void processPubComp(MqttAdapterMessage msg);

    void processDisconnect(MqttAdapterMessage msg);

    void processConnectionLost();

    void processSubscribe(MqttAdapterMessage msg);

    void processUnSubscribe(MqttAdapterMessage msg);

    void processPingReq(MqttAdapterMessage msg);

    void processAuthReq(MqttAdapterMessage msg);

    boolean connectionEstablished();
}
