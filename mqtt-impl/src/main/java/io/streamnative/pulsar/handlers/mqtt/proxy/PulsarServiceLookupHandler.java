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
package io.streamnative.pulsar.handlers.mqtt.proxy;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

/**
 * The proxy start with broker, use this lookup handler to find broker.
 */
@Slf4j
public class PulsarServiceLookupHandler implements LookupHandler {

    private PulsarService pulsarService;

    private PulsarClientImpl pulsarClient;

    public PulsarServiceLookupHandler(PulsarService pulsarService, PulsarClientImpl pulsarClient) {
        this.pulsarService = pulsarService;
        this.pulsarClient = pulsarClient;
    }

    @Override
    public CompletableFuture<Pair<String, Integer>> findBroker(TopicName topicName,
                                                               String protocolHandlerName) throws Exception {
        CompletableFuture<Pair<String, Integer>> lookupResult = new CompletableFuture<>();
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> lookup =
                pulsarClient.getLookup().getBroker(topicName);

        lookup.whenComplete((pair, throwable) -> {
            String hostName = pair.getLeft().getHostName();
            List<String> children = null;
            try {
                children = pulsarService.getZkClient().getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
            int mqttBrokerPort;

            for (String webService : children) {
                try {
                    byte[] content = pulsarService.getZkClient().getData(LoadManager.LOADBALANCE_BROKERS_ROOT
                            + "/" + webService, null, null);
                    ServiceLookupData serviceLookupData = pulsarService.getLoadManager().get()
                            .getLoadReportDeserializer().deserialize("", content);
                    if (serviceLookupData.getPulsarServiceUrl().contains("" + pair.getLeft().getPort())) {
                        if (serviceLookupData.getProtocol(protocolHandlerName).isPresent()) {
                            String mqttBrokerUrl = serviceLookupData.getProtocol(protocolHandlerName).get();
                            String[] splits = mqttBrokerUrl.split(":");
                            String port = splits[splits.length - 1];
                            mqttBrokerPort = Integer.parseInt(port);
                            lookupResult.complete(Pair.of(hostName, mqttBrokerPort));
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        });

        return lookupResult;
    }
}
