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

import io.streamnative.pulsar.handlers.mqtt.support.MQTTMetricsCollector;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.PulsarService;
import org.eclipse.jetty.http.HttpStatus;
/**
 * MQTT service servlet handler.
 */
public class MQTTServiceServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    // Define transient by spotbugs
    private final transient PulsarService pulsar;

    public MQTTServiceServlet(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setStatus(HttpStatus.OK_200);
        response.setContentType("text/plain");
        response.getOutputStream().write(getMetricsCollector().getJsonStats());
    }

    private MQTTMetricsCollector getMetricsCollector() {
        return ((MQTTProtocolHandler) pulsar.getProtocolHandlers().protocol("mqtt"))
                .getMqttService()
                .getMetricsCollector();
    }
}
