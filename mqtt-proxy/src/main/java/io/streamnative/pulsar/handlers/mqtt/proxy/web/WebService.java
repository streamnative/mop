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
package io.streamnative.pulsar.handlers.mqtt.proxy.web;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.jetty.JettyStatisticsCollector;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import io.streamnative.pulsar.handlers.mqtt.proxy.impl.MQTTProxyException;
import lombok.Getter;
import org.apache.pulsar.broker.web.DynamicSkipUnknownPropertyHandler;
import org.apache.pulsar.broker.web.GzipHandlerUtil;
import org.apache.pulsar.broker.web.JettyRequestLogFactory;
import org.apache.pulsar.broker.web.JsonMapperProvider;
import org.apache.pulsar.broker.web.UnrecognizedPropertyExceptionMapper;
import org.apache.pulsar.broker.web.WebExecutorThreadPool;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.ConnectionLimit;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ProxyConnectionFactory;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

/**
 * Web Service embedded into MoP Proxy.
 */
public class WebService implements AutoCloseable {

    private static final String MATCH_ALL = "/*";

    public static final String ATTRIBUTE_PROXY_NAME = "mop-proxy";
    public static final String HANDLER_CACHE_CONTROL = "max-age=3600";

    private final Server server;
    private final List<Handler> handlers;

    private final WebExecutorThreadPool webServiceExecutor;

    private final ServerConnector httpConnector;
    private JettyStatisticsCollector jettyStatisticsCollector;
    private PulsarSslFactory sslFactory;
    private ScheduledFuture<?> sslContextRefreshTask;
    private final MQTTCommonConfiguration config;
    private final MQTTProxyService proxyService;

    @Getter
    private static final DynamicSkipUnknownPropertyHandler sharedUnknownPropertyHandler =
            new DynamicSkipUnknownPropertyHandler();

    public void updateHttpRequestsFailOnUnknownPropertiesEnabled(boolean httpRequestsFailOnUnknownPropertiesEnabled){
        sharedUnknownPropertyHandler
                .setSkipUnknownProperty(!httpRequestsFailOnUnknownPropertiesEnabled);
    }

    public WebService(MQTTProxyService proxyService) {
        this.handlers = new ArrayList<>();
        this.config = proxyService.getProxyConfig();
        this.proxyService = proxyService;
        this.webServiceExecutor = new WebExecutorThreadPool(
                config.getNumHttpServerThreads(),
                "mop-web",
                config.getHttpServerThreadPoolQueueSize());
        this.server = new Server(webServiceExecutor);
        if (config.getMaxHttpServerConnections() > 0) {
            server.addBean(new ConnectionLimit(config.getMaxHttpServerConnections(), server));
        }
        List<ServerConnector> connectors = new ArrayList<>();

        Optional<Integer> port = config.getMopWebServicePort();
        HttpConfiguration httpConfig = new HttpConfiguration();
        if (config.isWebServiceTrustXForwardedFor()) {
            httpConfig.addCustomizer(new ForwardedRequestCustomizer());
        }
        httpConfig.setRequestHeaderSize(config.getHttpMaxRequestHeaderSize());
        HttpConnectionFactory httpConnectionFactory = new HttpConnectionFactory(httpConfig);
        if (port.isPresent()) {
            List<ConnectionFactory> connectionFactories = new ArrayList<>();
            if (config.isWebServiceHaProxyProtocolEnabled()) {
                connectionFactories.add(new ProxyConnectionFactory());
            }
            connectionFactories.add(httpConnectionFactory);
            httpConnector = new ServerConnector(server, connectionFactories.toArray(new ConnectionFactory[0]));
            httpConnector.setPort(port.get());
            httpConnector.setHost(config.getBindAddress());
            connectors.add(httpConnector);
        } else {
            httpConnector = null;
        }

        // Limit number of concurrent HTTP connections to avoid getting out of file descriptors
        connectors.forEach(c -> c.setAcceptQueueSize(config.getHttpServerAcceptQueueSize()));
        server.setConnectors(connectors.toArray(new ServerConnector[connectors.size()]));

        // Whether to reject requests with unknown attributes.
        sharedUnknownPropertyHandler.setSkipUnknownProperty(!config.isHttpRequestsFailOnUnknownPropertiesEnabled());

        addWebServerHandlers();
    }

    public void addRestResources(String basePath, boolean requiresAuthentication, Map<String, Object> attributeMap,
                                 boolean useSharedJsonMapperProvider, String... javaPackages) {
        ResourceConfig config = new ResourceConfig();
        for (String javaPackage : javaPackages) {
            config.packages(false, javaPackage);
        }
        addResourceServlet(basePath, requiresAuthentication, attributeMap, config, useSharedJsonMapperProvider);
    }

    public void addRestResource(String basePath, boolean requiresAuthentication, Map<String, Object> attributeMap,
                                boolean useSharedJsonMapperProvider, Class<?>... resourceClasses) {
        ResourceConfig config = new ResourceConfig();
        for (Class<?> resourceClass : resourceClasses) {
            config.register(resourceClass);
        }
        addResourceServlet(basePath, requiresAuthentication, attributeMap, config, useSharedJsonMapperProvider);
    }

    private void addResourceServlet(String basePath, boolean requiresAuthentication, Map<String, Object> attributeMap,
                                    ResourceConfig config, boolean useSharedJsonMapperProvider) {
        if (useSharedJsonMapperProvider){
            JsonMapperProvider jsonMapperProvider = new JsonMapperProvider(sharedUnknownPropertyHandler);
            config.register(jsonMapperProvider);
            config.register(UnrecognizedPropertyExceptionMapper.class);
        } else {
            config.register(JsonMapperProvider.class);
        }
        config.register(MultiPartFeature.class);
        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));
        servletHolder.setAsyncSupported(true);
        addServlet(basePath, servletHolder, requiresAuthentication, attributeMap);
    }

    public void addServlet(String path, ServletHolder servletHolder, boolean requiresAuthentication,
                           Map<String, Object> attributeMap) {
        ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        // Notice: each context path should be unique, but there's nothing here to verify that
        servletContextHandler.setContextPath(path);
        servletContextHandler.addServlet(servletHolder, MATCH_ALL);
        if (attributeMap != null) {
            attributeMap.forEach(servletContextHandler::setAttribute);
        }
        handlers.add(servletContextHandler);
    }

    public void addStaticResources(String basePath, String resourcePath) {
        ContextHandler capHandler = new ContextHandler();
        capHandler.setContextPath(basePath);
        ResourceHandler resHandler = new ResourceHandler();
        resHandler.setBaseResource(Resource.newClassPathResource(resourcePath));
        resHandler.setEtags(true);
        resHandler.setCacheControl(WebService.HANDLER_CACHE_CONTROL);
        capHandler.setHandler(resHandler);
        handlers.add(capHandler);
    }

    public void start() throws MQTTProxyException {
        try {
            RequestLogHandler requestLogHandler = new RequestLogHandler();
            RequestLog requestLogger = JettyRequestLogFactory.createRequestLogger(false, server);
            requestLogHandler.setRequestLog(requestLogger);
            handlers.add(0, new ContextHandlerCollection());
            handlers.add(requestLogHandler);

            ContextHandlerCollection contexts = new ContextHandlerCollection();
            contexts.setHandlers(handlers.toArray(new Handler[handlers.size()]));

            Handler handlerForContexts = GzipHandlerUtil.wrapWithGzipHandler(contexts,
                    config.getHttpServerGzipCompressionExcludedPaths());
            HandlerCollection handlerCollection = new HandlerCollection();
            handlerCollection.setHandlers(new Handler[] {handlerForContexts, new DefaultHandler(), requestLogHandler});

            // Metrics handler
            StatisticsHandler stats = new StatisticsHandler();
            stats.setHandler(handlerCollection);
            try {
                jettyStatisticsCollector = new JettyStatisticsCollector(stats);
                jettyStatisticsCollector.register();
            } catch (IllegalArgumentException e) {
                // Already registered. Eg: in unit tests
            }

            server.setHandler(stats);
            server.start();

            if (httpConnector != null) {
                log.info("MoP HTTP Service started at http://{}:{}", httpConnector.getHost(), httpConnector.getLocalPort());
            } else {
                log.info("MoP HTTP Service disabled");
            }

        } catch (Exception e) {
            throw new MQTTProxyException(e);
        }
    }

    private void addWebServerHandlers() {
        Map<String, Object> attributeMap = new HashMap<>();
        attributeMap.put(ATTRIBUTE_PROXY_NAME, proxyService);

        addRestResources("/admin",
                true, attributeMap, false,
                "io.streamnative.pulsar.handlers.mqtt.proxy.web.admin");
    }

    @Override
    public void close() throws MQTTProxyException {
        try {
            server.stop();
            // unregister statistics from Prometheus client's default CollectorRegistry singleton
            // to prevent memory leaks in tests
            if (jettyStatisticsCollector != null) {
                try {
                    CollectorRegistry.defaultRegistry.unregister(jettyStatisticsCollector);
                } catch (Exception e) {
                    // ignore any exception happening in unregister
                    // exception will be thrown for 2. instance of WebService in tests since
                    // the register supports a single JettyStatisticsCollector
                }
                jettyStatisticsCollector = null;
            }
            webServiceExecutor.join();
            if (this.sslContextRefreshTask != null) {
                this.sslContextRefreshTask.cancel(true);
            }
            log.info("Web service closed");
        } catch (Exception e) {
            throw new MQTTProxyException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WebService.class);
}
