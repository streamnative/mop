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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
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

    private static volatile Pair<Object, Method> metricsCollectorRef;

    private static final Object LOCK = new Object();

    public MQTTServiceServlet(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setStatus(HttpStatus.OK_200);
        response.setContentType("text/plain");
        response.getOutputStream().write(getJsonStats());
    }

    private byte[] getJsonStats() {
        try {
            Pair<Object, Method> metricsCollector = getMetricsCollector();
            return (byte[]) metricsCollector.getRight().invoke(metricsCollector.getLeft());
        } catch (Throwable ex) {
            return ex.getMessage().getBytes(StandardCharsets.UTF_8);
        }
    }

    private Pair<Object, Method> getMetricsCollector() throws IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        if (metricsCollectorRef == null) {
            synchronized (LOCK) {
                if (metricsCollectorRef == null) {
                    ProtocolHandler protocolHandler = pulsar.getProtocolHandlers().protocol("mqtt");
                    Method mqttServiceMethod = getMethod(protocolHandler.getClass(), "getMqttService");
                    Object mqttService = mqttServiceMethod.invoke(protocolHandler);
                    Method metricsCollectorMethod = getMethod(mqttService.getClass(), "getMetricsCollector");
                    Object metricsCollector = metricsCollectorMethod.invoke(mqttService);
                    Method jsonStatsMethod = getMethod(metricsCollector.getClass(), "getJsonStats");
                    metricsCollectorRef = Pair.of(metricsCollector, jsonStatsMethod);
                }
            }
        }
        return metricsCollectorRef;
    }

    private Method getMethod(Class<?> clazz, String methodName) throws NoSuchMethodException {
        Method method = clazz.getMethod(methodName);
        method.setAccessible(true);
        return method;
    }
}
