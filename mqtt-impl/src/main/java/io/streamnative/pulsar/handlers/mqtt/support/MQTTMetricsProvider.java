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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * MQTT metrics provider.
 */
public class MQTTMetricsProvider implements PrometheusRawMetricsProvider {

    @Getter
    private final MQTTMetricsCollector metricsCollector;

    public MQTTMetricsProvider(MQTTMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }

    @Override
    public void generate(SimpleTextOutputStream stream) {
        String cluster = metricsCollector.getServerConfiguration().getClusterName();
        Collection<Metrics> metrics = metricsCollector.getMetrics();
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
}
