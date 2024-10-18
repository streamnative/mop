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

import com.google.common.base.Splitter;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.PSKEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

    private volatile Object mqttService;

    private static final Object LOCK = new Object();

    public MQTTServiceServlet(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/plain");
        if ("/stats".equals(getRequestPath(request))) {
            response.setStatus(HttpStatus.OK_200);
            response.getOutputStream().write(getJsonStats());
        } else {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        if ("/add_psk_identity".equals(getRequestPath(request))) {
            response.setStatus(HttpStatus.OK_200);
            response.setContentType("text/plain");
            String data = readData(request);
            String paramName = "identity";
            String identity = Splitter.on("&")
                                    .splitToList(data)
                                    .stream()
                                    .filter(param -> param.startsWith(paramName))
                                    .findFirst().map(v -> v.substring(paramName.length() + 1)).orElse("");
            String result = "OK";
            if (StringUtils.isNotBlank(identity)) {
                result = invokeEventService(identity);
            }
            response.getOutputStream().write(result.getBytes(StandardCharsets.UTF_8));
        } else {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private String getRequestPath(HttpServletRequest request) {
        return request.getRequestURI().substring(request.getContextPath().length());
    }

    private String readData(HttpServletRequest request) throws IOException {
        StringBuilder data = new StringBuilder();
        String line;
        try (BufferedReader reader = request.getReader()) {
            while (null != (line = reader.readLine())) {
                data.append(line);
            }
        }
        return URLDecoder.decode(data.toString(), "UTF-8");
    }

    private String invokeEventService(String identity) {
        try {
            Method eventServiceMethod = getMethod(getMqttService().getClass(), "getEventService");
            Object eventService = eventServiceMethod.invoke(getMqttService());
            if (eventService != null) {
                Method sendPSKEvent = getMethod(eventService.getClass(), "sendPSKEvent", PSKEvent.class);
                PSKEvent event = new PSKEvent();
                event.setIdentity(identity);
                sendPSKEvent.invoke(eventService, event);
                return "OK";
            } else {
                return "Not supported in standalone mode, please enable proxy";
            }
        } catch (Throwable ex) {
            return ex.getMessage();
        }
    }

    private byte[] getJsonStats() {
        try {
            Pair<Object, Method> metricsCollector = getMetricsCollector();
            return (byte[]) metricsCollector.getRight().invoke(metricsCollector.getLeft());
        } catch (Throwable ex) {
            return ex.getMessage().getBytes(StandardCharsets.UTF_8);
        }
    }

    private Object getMqttService() throws IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        if (mqttService == null) {
            synchronized (LOCK) {
                if (mqttService == null) {
                    ProtocolHandler protocolHandler = pulsar.getProtocolHandlers().protocol("mqtt");
                    Method mqttServiceMethod = getMethod(protocolHandler.getClass(), "getMqttService");
                    mqttService = mqttServiceMethod.invoke(protocolHandler);
                }
            }
        }
        return mqttService;
    }

    private Pair<Object, Method> getMetricsCollector() throws IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        if (metricsCollectorRef == null) {
            synchronized (LOCK) {
                if (metricsCollectorRef == null) {
                    Method metricsCollectorMethod = getMethod(getMqttService().getClass(), "getMetricsCollector");
                    Object metricsCollector = metricsCollectorMethod.invoke(getMqttService());
                    Method jsonStatsMethod = getMethod(metricsCollector.getClass(), "getJsonStats");
                    metricsCollectorRef = Pair.of(metricsCollector, jsonStatsMethod);
                }
            }
        }
        return metricsCollectorRef;
    }

    private Method getMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes)
            throws NoSuchMethodException {
        Method method = clazz.getMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method;
    }
}
