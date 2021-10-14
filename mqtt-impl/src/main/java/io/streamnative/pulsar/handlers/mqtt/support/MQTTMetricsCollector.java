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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.MopVersion;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.impl.schema.writer.JacksonJsonWriter;
import org.apache.pulsar.common.stats.Metrics;

/**
 * MQTT metrics collector.
 */
public class MQTTMetricsCollector {

    @Getter
    private ClientMetricsRecord clientMetrics = new ClientMetricsRecord();
    @Getter
    private SubscriptionMetricsRecord subMetrics = new SubscriptionMetricsRecord();
    @Getter
    private MessageMetricsRecord msgMetrics = new MessageMetricsRecord();
    @Getter
    private final MQTTServerConfiguration serverConfiguration;

    private List<Metrics> metrics;

    private JacksonJsonWriter jsonWriter;

    private long startTime;

    public MQTTMetricsCollector(MQTTServerConfiguration config) {
        this.serverConfiguration = config;
        this.metrics = Lists.newArrayList();
        this.jsonWriter = new JacksonJsonWriter(new ObjectMapper());
        this.startTime = System.currentTimeMillis();
    }

    public List<Metrics> getMetrics() {
        Map<String, String> dimensionMap = new HashMap<>();
        Metrics m = Metrics.create(dimensionMap);
        m.put("mop_active_client_count", clientMetrics.getActiveCount());
        m.put("mop_total_client_count", clientMetrics.getTotalCount());
        m.put("mop_maximum_client_count", clientMetrics.getMaximumCount());
        m.put("mop_sub_count", subMetrics.getCount());
        m.put("mop_send_count", msgMetrics.getSendCount());
        m.put("mop_send_bytes", msgMetrics.getSendBytes());
        m.put("mop_received_count", msgMetrics.getReceivedCount());
        m.put("mop_received_bytes", msgMetrics.getReceivedBytes());

        metrics.clear();
        metrics.add(m);
        return metrics;
    }

    public byte[] getJsonStats() {
        Map<String, Object> broker = new HashMap<>();
        broker.put("version", MopVersion.getVersion());
        broker.put("uptime", (System.currentTimeMillis() - startTime) / 1000 + " seconds");
        broker.put("cluster", serverConfiguration.getClusterName());
        broker.put("tenant", serverConfiguration.getDefaultTenant());
        broker.put("namespace", serverConfiguration.getDefaultNamespace());

        Map<String, Object> clients = new HashMap<>();
        clients.put("total", clientMetrics.getTotalCount());
        clients.put("maximum", clientMetrics.getMaximumCount());
        clients.put("active", clientMetrics.getActiveCount());
        clients.put("active_clients", clientMetrics.getActiveClients());

        Map<String, Object> subscriptions = new HashMap<>();
        subscriptions.put("count", subMetrics.getCount());
        subscriptions.put("subs", subMetrics.getSubs());

        Map<String, Object> messages = new HashMap<>();
        messages.put("send_count", msgMetrics.getSendCount());
        messages.put("send_bytes", msgMetrics.getSendBytes());
        messages.put("received_count", msgMetrics.getReceivedCount());
        messages.put("received_bytes", msgMetrics.getReceivedBytes());

        broker.put("clients", clients);
        broker.put("subscriptions", subscriptions);
        broker.put("messages", messages);
        return jsonWriter.write(broker);
    }

    public void addClient(String address) {
        clientMetrics.addClient(address);
    }

    public void removeClient(String address) {
        clientMetrics.removeClient(address);
    }

    public void addSub(String sub) {
        subMetrics.addSub(sub);
    }

    public void removeSub(String sub) {
        subMetrics.removeSub(sub);
    }

    public void addSend(long bytes) {
        msgMetrics.addSend(bytes);
    }

    public void addReceived(long bytes) {
        msgMetrics.addReceived(bytes);
    }

    static class MessageMetricsRecord {

        private AtomicLong sendCount = new AtomicLong();
        private AtomicLong sendBytes = new AtomicLong();
        private AtomicLong receivedCount = new AtomicLong();
        private AtomicLong receivedBytes = new AtomicLong();

        public void addSend(long bytes) {
            sendBytes.addAndGet(bytes);
            sendCount.incrementAndGet();
        }

        public void addReceived(long bytes) {
            receivedBytes.addAndGet(bytes);
            receivedCount.incrementAndGet();
        }

        public long getSendCount() {
            return sendCount.get();
        }

        public long getSendBytes() {
            return sendBytes.get();
        }

        public long getReceivedCount() {
            return receivedCount.get();
        }

        public long getReceivedBytes() {
            return receivedBytes.get();
        }
    }

    static class SubscriptionMetricsRecord {

        private AtomicLong count = new AtomicLong();
        private Set<String> subs = ConcurrentHashMap.newKeySet();

        public void addSub(String sub) {
            if (subs.add(sub)) {
                count.incrementAndGet();
            }
        }

        public void removeSub(String sub) {
            if (subs.remove(sub)) {
                count.decrementAndGet();
            }
        }

        public long getCount() {
            return count.get();
        }

        public Set<String> getSubs() {
            return subs;
        }
    }

    static class ClientMetricsRecord {

        private AtomicLong activeCount = new AtomicLong();
        private AtomicLong totalCount = new AtomicLong();
        private AtomicLong maximumCount = new AtomicLong();
        private Set<String> activeClients = ConcurrentHashMap.newKeySet();

        public long getActiveCount() {
            return activeCount.get();
        }

        public long getTotalCount() {
            return totalCount.get();
        }

        public long getMaximumCount() {
            return maximumCount.get();
        }

        public Set<String> getActiveClients() {
            return activeClients;
        }

        public void addClient(String address) {
            if (activeClients.add(address)) {
                long current = activeCount.incrementAndGet();
                if (current > maximumCount.get()) {
                    maximumCount.set(current);
                }
                totalCount.incrementAndGet();
            }
        }

        public void removeClient(String address) {
            if (StringUtils.isNotEmpty(address) && activeClients.remove(address)) {
                activeCount.decrementAndGet();
            }
        }
    }
}
