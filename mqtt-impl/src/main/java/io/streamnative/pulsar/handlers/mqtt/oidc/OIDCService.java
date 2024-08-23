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
package io.streamnative.pulsar.handlers.mqtt.oidc;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class OIDCService {

    static final char PRINCIPAL_JOINER = (char) 27;

    private final PulsarService pulsarService;

    private final MetadataStore metadataStore;

    private final OIDCPoolResources poolResources;

    private final ConcurrentHashMap<String, List<PoolCompiler>> poolsMap = new ConcurrentHashMap<>();

    private final List<PoolCompiler> pools = new ArrayList<>();

    public OIDCService(PulsarService pulsarService) throws MetadataStoreException {
        this.pulsarService = pulsarService;
//        this.metadataStore = createLocalMetadataStore(pulsarService.getConfig());
        this.metadataStore = pulsarService.getConfigurationMetadataStore();
        this.metadataStore.registerListener(this::handleMetadataChanges);
        this.poolResources = new OIDCPoolResources(this.metadataStore);
        this.createData();
    }

    public MetadataStoreExtended createLocalMetadataStore(ServiceConfiguration config) throws MetadataStoreException {
        return MetadataStoreExtended.create(config.getMetadataStoreUrl(),
                MetadataStoreConfig.builder()
                        .sessionTimeoutMillis((int) config.getMetadataStoreSessionTimeoutMillis())
                        .allowReadOnlyOperations(config.isMetadataStoreAllowReadOnlyOperations())
                        .configFilePath(config.getMetadataStoreConfigPath())
                        .batchingEnabled(config.isMetadataStoreBatchingEnabled())
                        .batchingMaxDelayMillis(config.getMetadataStoreBatchingMaxDelayMillis())
                        .batchingMaxOperations(config.getMetadataStoreBatchingMaxOperations())
                        .batchingMaxSizeKb(config.getMetadataStoreBatchingMaxSizeKb())
                        .metadataStoreName(MetadataStoreConfig.METADATA_STORE)
                        .build());
    }

    private CompletableFuture<Void> createData() {
        Pool pool = new Pool("test-pool", "d", "provider-1", "claims.CN=='CLIENT'");
        return doLoadPools(CompletableFuture.completedFuture(Lists.newArrayList(pool)));

    }

    private CompletableFuture<Void> createDemoData() {
        Pool pool = new Pool("test-pool", "d", "provider-1", "claims.CN=='CLIENT'");
        return poolResources.createPoolAsync(pool);
    }

    private CompletableFuture<Void> loadPoolsAsync() {
        return doLoadPools(poolResources.listPoolsAsync());
    }

    private CompletableFuture<Void> loadPoolAsync(String pool) {
        final CompletableFuture<List<Pool>> pools = poolResources.getPoolAsync(pool).thenCompose(opt -> {
            if (opt.isEmpty()) {
                return CompletableFuture.completedFuture(new ArrayList<>());
            }
            return CompletableFuture.completedFuture(List.of(opt.get()));
        });
        return doLoadPools(pools);
    }

    private CompletableFuture<Void> doLoadPools(CompletableFuture<List<Pool>> future) {
        return future.thenApply(localPools -> {
                    localPools.stream().forEach(p -> {
                        final List<PoolCompiler> poolCompilers = poolsMap.computeIfAbsent(p.providerName(),
                                k -> Collections.synchronizedList(new ArrayList<>()));
                        poolCompilers.removeIf(pc -> pc.getName().equalsIgnoreCase(p.name()));
                        PoolCompiler compiler = new PoolCompiler(p);
                        poolCompilers.add(compiler);
                        //
                        pools.removeIf(pc -> pc.getName().equalsIgnoreCase(p.name()));
                        pools.add(compiler);
                    });

                    List<String> poolsNames = new ArrayList<>();
                    for (List<PoolCompiler> value : poolsMap.values()) {
                        Optional<PoolCompiler> poolCompiler = value.stream().findFirst();
                        poolCompiler.ifPresent(compiler -> poolsNames.add(compiler.getName()));
                    }
                    return poolsNames;
                }).thenAccept(poolsNames -> log.info("refreshed OIDC pools, pools: {}, {}", poolsNames, pools))
                .exceptionally(ex -> {
                    log.error("load pool error", ex);
                    return null;
                });
    }

    public String getPrincipal(Map<String, Object> datas) {
        List<String> principals = new ArrayList<>();
        for (PoolCompiler pool : pools) {
            ExpressionCompiler compiler = pool.getCompiler();
            Map<String, Object> variables = new HashMap<>();
            Map<String, Object> claims = new HashMap<>();
            for (String var : compiler.getVariables()) {
                Object jwtValue = datas.get(var);
                if (jwtValue != null) {
                    variables.put(var, jwtValue);
                }
            }
            claims.put("claims", variables);
            Boolean matched = false;
            try {
                matched = compiler.eval(claims);
            } catch (Exception e) {
                log.warn("eval : {} value : {}", pool.getExpression(), claims, e);
            }
            if (matched) {
                principals.add(pool.getName());
            }
        }
        return Joiner.on(PRINCIPAL_JOINER).join(principals);
    }

    private void handleMetadataChanges(Notification n) {
        if (OIDCPoolResources.pathIsFromPool(n.getPath())) {
            log.info("pool-handleMetadataChanges : {}", n);
            handlePoolAsync(n);
        }
    }

    private void handlePoolAsync(Notification n) {
        String pool = OIDCPoolResources.poolFromPath(n.getPath());
        if (NotificationType.Created == n.getType() || NotificationType.Modified == n.getType()) {
            loadPoolAsync(pool);
        } else if (NotificationType.Deleted == n.getType()) {
            deletePool(pool);
        }
    }

    private void deletePool(String pool) {
        String providerName = "";
        for (List<PoolCompiler> pools : poolsMap.values()) {
            Iterator<PoolCompiler> iterator = pools.iterator();
            while (iterator.hasNext()) {
                final PoolCompiler item = iterator.next();
                if (item.getName().equalsIgnoreCase(pool)) {
                    iterator.remove();
                    providerName = item.getProviderName();
                }
            }
        }
        if (StringUtils.isNotEmpty(providerName)) {
            final List<PoolCompiler> poolCompilers = poolsMap.get(providerName);
            if (CollectionUtils.isEmpty(poolCompilers)) {
                poolsMap.remove(providerName);
            }
        }
    }
}
