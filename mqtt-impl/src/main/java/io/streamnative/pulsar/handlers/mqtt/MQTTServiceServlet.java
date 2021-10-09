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

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTMetricsProvider;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.pulsar.broker.PulsarService;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT service servlet handler.
 */
public class MQTTServiceServlet extends HttpServlet {

    private static final Logger log = LoggerFactory.getLogger(MQTTServiceServlet.class);

    private static final long serialVersionUID = 1L;

    private final long metricsServletTimeoutMs;

    // Define transient by spotbugs
    private transient ExecutorService executor;

    private final transient PulsarService pulsar;

    public MQTTServiceServlet(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.metricsServletTimeoutMs = pulsar.getConfiguration().getMetricsServletTimeoutMs();
    }

    @Override
    public void init() throws ServletException {
        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("mop-stats"));
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        AsyncContext context = request.startAsync();
        context.setTimeout(metricsServletTimeoutMs);
        executor.execute(safeRun(() -> {
            HttpServletResponse res = (HttpServletResponse) context.getResponse();
            try {
                res.setStatus(HttpStatus.OK_200);
                res.setContentType("text/plain");
                getMetricsProvider().generate(res.getOutputStream());
                context.complete();
            } catch (Exception e) {
                log.error("Failed to generate mop stats", e);
                res.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
                context.complete();
            }
        }));
    }

    @Override
    public void destroy() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private MQTTMetricsProvider getMetricsProvider() {
        return ((MQTTProtocolHandler) pulsar.getProtocolHandlers().protocol("mqtt"))
                .getMqttService()
                .getMetricsProvider();
    }
}
