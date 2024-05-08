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
package io.streamnative.oidc.broker.common;

import static io.streamnative.oidc.broker.common.OIDCConstants.RESOURCE_SYNC_OPERATION_TIMEOUT_SEC;

import com.fasterxml.jackson.core.type.TypeReference;
import io.streamnative.oidc.broker.common.pojo.Provider;
import io.streamnative.oidc.broker.common.utils.Paths;
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
public final class OIDCProviderResources extends BaseResources<Provider> {
    private static final String BASE_PATH = "/sn-oidc/providers";

    public OIDCProviderResources(@NotNull MetadataStore metadataStore) {
        super(metadataStore, new TypeReference<>() { }, RESOURCE_SYNC_OPERATION_TIMEOUT_SEC);
    }

    public @NotNull Optional<Provider> getProvider(@NotNull String providerName) throws MetadataStoreException {
        return get(joinPath(BASE_PATH, Paths.getUrlEncodedPath(providerName)));
    }

    public @NotNull CompletableFuture<Optional<Provider>> getProviderAsync(@NotNull String providerName) {
        return getAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(providerName)));
    }

    public void createProvider(@NotNull Provider provider) throws MetadataStoreException  {
        create(joinPath(BASE_PATH, Paths.getUrlEncodedPath(provider.name())), provider);
    }

    public @NotNull CompletableFuture<Void> createProviderAsync(@NotNull Provider provider) {
        return createAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(provider.name())), provider);
    }

    public @NotNull CompletableFuture<Boolean> existsAsync(@NotNull String providerName) {
        return super.existsAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(providerName)));
    }

    public void deleteProvider(@NotNull String providerName) throws MetadataStoreException {
        super.delete(joinPath(BASE_PATH, Paths.getUrlEncodedPath(providerName)));
    }

    public @NotNull CompletableFuture<Void> deleteProviderAsync(@NotNull String providerName) {
        return super.deleteIfExistsAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(providerName)));
    }

    public @NotNull CompletableFuture<Void> updateProviderAsync(@NotNull Provider provider) {
        return super.setAsync(joinPath(BASE_PATH, Paths.getUrlEncodedPath(provider.name())), __ -> provider);
    }

    public void updateProvider(@NotNull Provider provider) throws MetadataStoreException {
        super.set(joinPath(BASE_PATH, Paths.getUrlEncodedPath(provider.name())), __ -> provider);
    }

    public @NotNull CompletableFuture<List<String>> listProviderNamesAsync() {
        return super.getChildrenAsync(joinPath(BASE_PATH));
    }

    public @NotNull CompletableFuture<List<Provider>> listProvidersAsync() {
        return super.getChildrenAsync(joinPath(BASE_PATH))
                .thenCompose(providerNames -> {
                    List<CompletableFuture<Optional<Provider>>> providers = new ArrayList<>();
                    for (String name : providerNames) {
                        providers.add(getAsync(joinPath(BASE_PATH, name)));
                    }
                    return FutureUtil.waitForAll(providers)
                            .thenApply(__ -> providers.stream().map(f -> f.join())
                                    .filter(f -> f.isPresent()).map(f -> f.get())
                                    .collect(Collectors.toList()));
                });
    }

    public static boolean pathIsFromProvider(String path) {
        return path.startsWith(BASE_PATH + "/");
    }

    public static String providerFromPath(String path) {
        return path.substring(BASE_PATH.length() + 1);
    }
}
