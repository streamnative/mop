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

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import io.streamnative.pulsar.handlers.mqtt.utils.ReflectionUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.eclipse.jetty.http.HttpStatus;

/**
 * MQTT service servlet handler.
 */
@Slf4j
public class MQTTServiceServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    // Define transient by spotbugs
    private final transient PulsarService pulsar;
    private volatile ReflectionUtils.Reference metricsCollectorGetJsonStatsReference;
    private static final AtomicReferenceFieldUpdater<MQTTServiceServlet, ReflectionUtils.Reference>
            JSON_STATS_REFERENCE_UPDATER = newUpdater(MQTTServiceServlet.class, ReflectionUtils.Reference.class,
            "metricsCollectorGetJsonStatsReference");


    public MQTTServiceServlet(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        checkAndInitReflectionReference();
        response.setStatus(HttpStatus.OK_200);
        response.setContentType("text/plain");
        try {
            response.getOutputStream().write((byte[]) JSON_STATS_REFERENCE_UPDATER.get(this).invoke());
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * Lazy init reference updater.
     */
    private void checkAndInitReflectionReference() {
        if (metricsCollectorGetJsonStatsReference == null) {
            try {
                ProtocolHandler protocolHandler = pulsar.getProtocolHandlers().protocol("mqtt");
                Method getMqttService = ReflectionUtils.
                        reflectAccessibleMethod(protocolHandler.getClass(), "getMqttService");
                Object mqttService = getMqttService.invoke(protocolHandler);
                Method getMetricsCollector = ReflectionUtils.
                        reflectAccessibleMethod(mqttService.getClass(), "getMetricsCollector");
                Object mqttMetricCollector = getMetricsCollector.invoke(mqttService);
                Method getJsonStats = ReflectionUtils.
                        reflectAccessibleMethod(mqttMetricCollector.getClass(), "getJsonStats");
                JSON_STATS_REFERENCE_UPDATER
                        .compareAndSet(this, null, ReflectionUtils.createReference(getJsonStats, mqttMetricCollector));
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("Reflect method got error", e);
                throw new IllegalStateException(e);
            }
        }
    }


}
