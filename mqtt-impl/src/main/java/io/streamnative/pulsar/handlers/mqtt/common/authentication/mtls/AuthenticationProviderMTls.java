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
package io.streamnative.pulsar.handlers.mqtt.common.authentication.mtls;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.streamnative.oidc.broker.common.OIDCPoolResources;
import io.streamnative.oidc.broker.common.pojo.Pool;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.naming.AuthenticationException;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class AuthenticationProviderMTls implements AuthenticationProvider {
    static final char PRINCIPAL_JOINER = (char) 27;
    static final String AUTH_METHOD_MTLS = "mtls";

    private MetadataStore metadataStore;
    @Getter
    @VisibleForTesting
    private OIDCPoolResources poolResources;

    private final ObjectMapper objectMapper = ObjectMapperFactory.create();

    @Getter
    @VisibleForTesting
    private final ConcurrentHashMap<String, ExpressionCompiler> poolMap = new ConcurrentHashMap<>();
    private boolean needCloseMetaData = false;
    private AuthenticationMetrics metrics;

    private enum ErrorCode {
        UNKNOWN,
        INVALID_CERTS,
        INVALID_DN,
        INVALID_SAN,
        NO_MATCH_POOL;

        ErrorCode() {
        }
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws IOException {
        initialize(Context.builder().config(conf).build());
    }

    @Override
    public void initialize(Context context) throws IOException {
        metrics = new AuthenticationMetrics(
                context.getOpenTelemetry(), getClass().getSimpleName(), "mtls");
        init(context.getConfig());
    }

    private void init(ServiceConfiguration conf) throws MetadataStoreException {
        this.metadataStore = createLocalMetadataStore(conf);
        this.needCloseMetaData = true;
        this.metadataStore.registerListener(this::handleMetadataChanges);
        this.poolResources = new OIDCPoolResources(metadataStore);
        this.loadAsync();
    }

    public void initialize(MetadataStore metadataStore) {
        Context context = Context.builder().build();
        this.metrics = new AuthenticationMetrics(
                context.getOpenTelemetry(), getClass().getSimpleName(), "mtls");
        this.metadataStore = metadataStore;
        this.metadataStore.registerListener(this::handleMetadataChanges);
        this.poolResources = new OIDCPoolResources(metadataStore);
        this.loadAsync();
    }

    public MetadataStoreExtended createLocalMetadataStore(ServiceConfiguration config) throws
        MetadataStoreException {
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

    private CompletableFuture<Void> loadAsync() {
        return loadPoolsAsync().thenAccept(__ -> log.info("loaded streamnative mtls authentication identitypools"));
    }

    private CompletableFuture<Void> loadPoolsAsync() {
        return doLoadPools(poolResources.listPoolsAsync());
    }

    private CompletableFuture<Void> doLoadPools(CompletableFuture<List<Pool>> future) {
        return future.thenApply(pools -> {
                pools.stream().forEach(p -> {
                    if (p.authType().equals(AUTH_METHOD_MTLS)) {
                        try {
                            poolMap.put(p.name(), new ExpressionCompiler(p.expression()));
                        } catch (Exception e) {
                            log.error("Failed to compile expression for pool: {}, expression: {}",
                                p.name(), p.expression(), e);
                        }
                    }
                });

                return poolMap.keySet();
            }).thenAccept(poolsNames -> log.info("refreshed mTls identity pools, pools: {}", poolsNames))
            .exceptionally(ex -> {
                log.error("load pool error", ex);
                return null;
            });
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
        poolMap.remove(pool);
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_METHOD_MTLS;
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        ErrorCode errorCode = ErrorCode.UNKNOWN;

        try {
            if (!authData.hasDataFromTls()) {
                errorCode = ErrorCode.INVALID_CERTS;
                throw new AuthenticationException("Failed to get TLS certificates from client");
            }

            Certificate[] certs = authData.getTlsCertificates();
            if (null == certs || certs.length == 0) {
                errorCode = ErrorCode.INVALID_CERTS;
                throw new AuthenticationException("Failed to get TLS certificates from client");
            }

            // Parse CEL params form client certificate, refer to:
            // https://docs.confluent.io/cloud/current/security/authenticate/
            // workload-identities/identity-providers/mtls/cel-filters.html
            final X509Certificate certificate = (X509Certificate) certs[0];

            // parse DN
            Map<String, String> params;
            try {
                String subject = certificate.getSubjectX500Principal().getName();
                params = parseDN(subject);
            } catch (Exception e) {
                errorCode = ErrorCode.INVALID_DN;
                throw new AuthenticationException("Failed to parse the DN from the client certificate");
            }

            // parse SAN
            parseSAN(certificate, params);
            // get SNID
            params.put(ExpressionCompiler.SNID, certificate.getSerialNumber().toString(16).toUpperCase());
            // parse SHA1
            params.put(ExpressionCompiler.SHA1, parseSHA1FingerPrint(certificate));

            String poolName = matchPool(params);
            if (poolName.isEmpty()) {
                errorCode = ErrorCode.NO_MATCH_POOL;
                throw new AuthenticationException("No matched identity pool from the client certificate");
            }
            AuthRequest authRequest = new AuthRequest(poolName, params);
            String authRequestJson = objectMapper.writeValueAsString(authRequest);
            metrics.recordSuccess();
            return authRequestJson;
        } catch (AuthenticationException e) {
            metrics.recordFailure(errorCode);
            throw e;
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize the auth request", e);
            metrics.recordFailure(errorCode);
            throw new AuthenticationException(e.getMessage());
        }
    }

    public String matchPool(Map<String, String> params) throws AuthenticationException {
        List<String> principals = new ArrayList<>();
        poolMap.forEach((poolName, compiler) -> {
            Boolean matched = false;
            try {
                matched = compiler.eval(params);
            } catch (Exception e) {
                log.warn("Failed to evaluate expression, eval : {} value : {}", compiler.getExpression(), params, e);
            }
            if (matched) {
                principals.add(poolName);
            }
        });
        return Joiner.on(PRINCIPAL_JOINER).join(principals);
    }


    @Override
    public void close() throws IOException {
        // no op
        if (needCloseMetaData) {
            try {
                metadataStore.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static String parseSHA1FingerPrint(X509Certificate certificate) {
        try {
            byte[] certBytes = certificate.getEncoded();

            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] sha1hash = md.digest(certBytes);

            return Hex.encodeHexString(sha1hash, false);
        } catch (Exception e) {
            log.error("Failed to parse the SHA-1 fingerprint from the client certificate", e);
            return "";
        }
    }

    static Map<String, String> parseDN(String dn) throws InvalidNameException {
        Map<String, String> params = new HashMap<>();
        if (StringUtils.isEmpty(dn)) {
            return params;
        }
        params.put(ExpressionCompiler.DN, dn);
        LdapName ldapName = new LdapName(dn);
        for (Rdn rdn : ldapName.getRdns()) {
            String rdnType = rdn.getType().toUpperCase();
            String value = Rdn.escapeValue(rdn.getValue());
            value = value.replace("\r", "\\0D");
            value = value.replace("\n", "\\0A");
            params.put(rdnType, value);
        }

        return params;
    }

    static void parseSAN(X509Certificate certificate, @NotNull Map<String, String> map) {
        try {
            // byte[] extensionValue = certificate.getExtensionValue("2.5.29.17");
            // TODO How to get the original extension name
            Collection<List<?>> subjectAlternativeNames = certificate.getSubjectAlternativeNames();
            if (subjectAlternativeNames != null) {
                List<String> formattedSANList = subjectAlternativeNames.stream()
                        .map(list -> {
                            String sanName = getSanName((int) list.get(0));
                            String sanValue = (String) list.get(1);
                            map.put(sanName, sanValue);
                            sanName = mapSANNames(sanName, sanValue, map);
                            return sanName + ":" + sanValue;
                        })
                        .collect(Collectors.toList());
                String formattedSAN = String.join(",", formattedSANList);
                map.put(ExpressionCompiler.SAN, formattedSAN);
            }
        } catch (Exception e) {
            log.error("Failed to parse the SAN from the client certificate, skip parse SAN", e);
        }
    }

    static String mapSANNames(String sanName, String sanValue, @NotNull Map<String, String> map) {
        String newSanName = sanName;
        // "RFC822NAME:aaa" -> "EMAIL:aaa,DEVICE_ID:aaa,RFC822NAME:aaa"
        if (sanName.equals("DNS")) {
            StrBuilder strBuilder = new StrBuilder();
            strBuilder.append("EMAIL:").append(sanValue).append(",");
            map.put("EMAIL", sanValue);

//            strBuilder.append("DEVICE_ID:").append(sanValue).append(",");
//            map.put("DEVICE_ID", sanValue);

            strBuilder.append(sanName);
            newSanName = strBuilder.toString();
        }
        return newSanName;
    }

    private static String getSanName(int type) {
        return switch (type) {
            case 0 -> "OTHERNAME";
            case 1 -> "RFC822NAME";
            case 2 -> "DNS";
            case 3 -> "X400";
            case 4 -> "DIR";
            case 5 -> "EDIPARTY";
            case 6 -> "URI";
            case 7 -> "IP";
            case 8 -> "RID";
            default -> "OTHERNAME";
        };
    }
}
