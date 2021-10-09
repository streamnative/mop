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

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * MQTT metrics provider.
 */
public class MQTTMetricsProvider implements PrometheusRawMetricsProvider {

    private final AtomicLong onlineClientsCount = new AtomicLong();

    private Set<String> onlineClients = ConcurrentHashMap.newKeySet();

    @Getter
    private final MQTTServerConfiguration serverConfiguration;

    private List<Metrics> metrics;

    public MQTTMetricsProvider(MQTTServerConfiguration config) {
        this.serverConfiguration = config;
        this.metrics = Lists.newArrayList();
    }

    @Override
    public void generate(SimpleTextOutputStream stream) {
        String cluster = serverConfiguration.getClusterName();
        Collection<Metrics> metrics = getMetrics();
        Set<String> names = new HashSet<>();
        for (Metrics item : metrics) {
            for (Map.Entry<String, Object> entry : item.getMetrics().entrySet()) {
                String name = entry.getKey();
                if (!names.contains(name)) {
                    stream.write("# TYPE ").write(entry.getKey()).write(' ')
                            .write("counter").write('\n');
                    names.add(name);
                }
                stream.write(name)
                        .write("{cluster=\"").write(cluster).write('"');

                for (Map.Entry<String, String> metric : item.getDimensions().entrySet()) {
                    if (metric.getKey().isEmpty() || "cluster".equals(metric.getKey())) {
                        continue;
                    }
                    stream.write(", ").write(metric.getKey()).write("=\"").write(metric.getValue()).write('"');
                }
                stream.write("} ").write(String.valueOf(entry.getValue()))
                        .write(' ').write(System.currentTimeMillis()).write("\n");
            }
        }
    }

    private Collection<Metrics> getMetrics() {
        Map<String, String> dimensionMap = new HashMap<>();
        Metrics m = Metrics.create(dimensionMap);
        m.put("mop_online_clients_count", getAndResetOnlineClientsCount());

        metrics.clear();
        metrics.add(m);
        return metrics;
    }

    public long getAndResetOnlineClientsCount() {
        return onlineClientsCount.getAndSet(0);
    }

    public long getOnlineClientsCount() {
        return onlineClientsCount.get();
    }

    public Set<String> getOnlineClients() {
        return onlineClients;
    }

    public void addClient(String address) {
        if (onlineClients.add(address)) {
            onlineClientsCount.incrementAndGet();
        }
    }

    public void removeClient(String address) {
        if (StringUtils.isNotEmpty(address) && onlineClients.remove(address)) {
            onlineClientsCount.decrementAndGet();
        }
    }

}
