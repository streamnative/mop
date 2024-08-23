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

import com.fasterxml.jackson.core.type.TypeReference;
import io.streamnative.pulsar.handlers.mqtt.utils.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import org.apache.pulsar.broker.resources.BaseResources;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

@SuppressWarnings("UnstableApiUsage")
public final class OIDCPoolResources extends BaseResources<Pool> {

    public static final int RESOURCE_SYNC_OPERATION_TIMEOUT_SEC = 30;
    private static final String BASE_PATH = "/sn-oidc/pools";

    public OIDCPoolResources(@NotNull MetadataStore metadataStore) {
        super(metadataStore, new TypeReference<>() { }, RESOURCE_SYNC_OPERATION_TIMEOUT_SEC);
    }

    public @NotNull Optional<Pool> getPool(@NotNull String poolName) throws MetadataStoreException {
        return get(joinPath(BASE_PATH, Paths.getUrlEncodedPath(poolName)));
    }

    public @NotNull CompletableFuture<Optional<Pool>> getPoolAsync(@NotNull String poolName) {
        return getAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(poolName)));
    }

    public void createPool(@NotNull Pool pool) throws MetadataStoreException {
        create(joinPath(BASE_PATH, Paths.getUrlEncodedPath(pool.name())), pool);
    }

    public @NotNull CompletableFuture<Void> createPoolAsync(@NotNull Pool pool) {
        return createAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(pool.name())), pool);
    }

    public @NotNull CompletableFuture<Boolean> existsAsync(@NotNull String poolName) {
        return super.existsAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(poolName)));
    }

    public void deletePool(@NotNull String poolName) throws MetadataStoreException {
        super.delete(joinPath(BASE_PATH, Paths.getUrlEncodedPath(poolName)));
    }

    public @NotNull CompletableFuture<Void> deletePoolAsync(@NotNull String poolName) {
        return super.deleteIfExistsAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(poolName)));
    }

    public @NotNull CompletableFuture<Void> updatePoolAsync(@NotNull Pool pool) {
        return super.setAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(pool.name())), __ -> pool);
    }

    public @NotNull CompletableFuture<List<String>> listPoolNamesAsync() {
        return super.getChildrenAsync(joinPath(BASE_PATH));
    }

    public @NotNull CompletableFuture<List<Pool>> listPoolsAsync() {
        return super.getChildrenAsync(joinPath(BASE_PATH))
                .thenCompose(poolNames -> {
                    List<CompletableFuture<Optional<Pool>>> pools = new ArrayList<>();
                    for (String name : poolNames) {
                        pools.add(getAsync(joinPath(BASE_PATH, name)));
                    }
                    return FutureUtil.waitForAll(pools)
                            .thenApply(__ -> pools.stream().map(f -> f.join())
                                    .filter(f -> f.isPresent())
                                    .map(f -> f.get())
                                    .collect(Collectors.toList()));
                });
    }

    public static boolean pathIsFromPool(String path) {
        return path.startsWith(BASE_PATH + "/");
    }

    public static String poolFromPath(String path) {
        return path.substring(BASE_PATH.length() + 1);
    }
}
