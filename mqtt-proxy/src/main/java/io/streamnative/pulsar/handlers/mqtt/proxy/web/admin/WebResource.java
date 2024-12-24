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
package io.streamnative.pulsar.handlers.mqtt.proxy.web.admin;

import static java.util.concurrent.TimeUnit.SECONDS;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import io.streamnative.pulsar.handlers.mqtt.proxy.web.WebService;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Web resources in Pulsar. It provides basic authorization functions.
 */
public abstract class WebResource {

    private static final Logger log = LoggerFactory.getLogger(PulsarWebResource.class);

    private static final LoadingCache<String, PulsarServiceNameResolver> SERVICE_NAME_RESOLVER_CACHE =
            Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(5)).build(
                    new CacheLoader<>() {
                        @Override
                        public @Nullable PulsarServiceNameResolver load(@NonNull String serviceUrl) throws Exception {
                            PulsarServiceNameResolver serviceNameResolver = new PulsarServiceNameResolver();
                            serviceNameResolver.updateServiceUrl(serviceUrl);
                            return serviceNameResolver;
                        }
                    });

    static final String ORIGINAL_PRINCIPAL_HEADER = "X-Original-Principal";

    @Context
    protected ServletContext servletContext;

    @Context
    protected HttpServletRequest httpRequest;

    @Context
    protected UriInfo uri;

    private MQTTProxyService service;

    protected MQTTProxyService service() {
        if (service == null) {
            service = (MQTTProxyService) servletContext.getAttribute(WebService.ATTRIBUTE_PROXY_NAME);
        }
        return service;
    }

    protected MQTTCommonConfiguration config() {
        return service().getProxyConfig();
    }

    /**
     * Gets a caller id (IP + role).
     *
     * @return the web service caller identification
     */
    public String clientAppId() {
        return (String) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
    }

    public String originalPrincipal() {
        return httpRequest.getHeader(ORIGINAL_PRINCIPAL_HEADER);
    }

    public AuthenticationDataSource clientAuthData() {
        return (AuthenticationDataSource) httpRequest.getAttribute(AuthenticationFilter.AuthenticatedDataAttributeName);
    }

    public boolean isRequestHttps() {
        return "https".equalsIgnoreCase(httpRequest.getScheme());
    }

    public static boolean isClientAuthenticated(String appId) {
        return appId != null;
    }


    public <T> T sync(Supplier<CompletableFuture<T>> supplier) {
        try {
            return supplier.get().get(config().getMetadataStoreOperationTimeoutSeconds(), SECONDS);
        } catch (ExecutionException | TimeoutException ex) {
            Throwable realCause = FutureUtil.unwrapCompletionException(ex);
            if (realCause instanceof WebApplicationException) {
                throw (WebApplicationException) realCause;
            } else {
                throw new RestException(realCause);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RestException(ex);
        }
    }

    protected static void resumeAsyncResponseExceptionally(AsyncResponse asyncResponse, Throwable exception) {
        Throwable realCause = FutureUtil.unwrapCompletionException(exception);
        if (realCause instanceof WebApplicationException) {
            asyncResponse.resume(realCause);
        } else if (realCause instanceof BrokerServiceException.NotAllowedException) {
            asyncResponse.resume(new RestException(Status.CONFLICT, realCause));
        } else if (realCause instanceof MetadataStoreException.NotFoundException) {
            asyncResponse.resume(new RestException(Status.NOT_FOUND, realCause));
        } else if (realCause instanceof MetadataStoreException.BadVersionException) {
            asyncResponse.resume(new RestException(Status.CONFLICT, "Concurrent modification"));
        } else if (realCause instanceof PulsarAdminException) {
            asyncResponse.resume(new RestException(((PulsarAdminException) realCause)));
        } else {
            asyncResponse.resume(new RestException(realCause));
        }
    }
}
