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
package io.streamnative.pulsar.handlers.mqtt.identitypool;

import io.netty.channel.local.LocalAddress;
import io.streamnative.oidc.broker.common.pojo.Pool;
import java.io.File;
import java.net.URL;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AuthenticationProviderMTlsTest {

    private static final String SUPER_USER = "superUser";
    private static final String CLUSTER = "mtls-test";

    private ServiceConfiguration serviceConfiguration;
    private LocalMemoryMetadataStore metadataStore;

    @SuppressWarnings("UnstableApiUsage")
    @BeforeClass
    public void setup() throws Exception {
        this.metadataStore = new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build());
        this.serviceConfiguration = new ServiceConfiguration();
        this.serviceConfiguration.setClusterName(CLUSTER);
        this.serviceConfiguration.setSuperUserRoles(Set.of(SUPER_USER));
        this.serviceConfiguration.setMetadataStoreUrl("memory:local");
    }

    @AfterClass
    public void cleanup() throws Exception {
        this.metadataStore.close();
    }

    @DataProvider
    public Object[][] reuseMetadata() {
        return new Object[][] {
            {true},
            {false}
        };
    }

    @Test
    public void testExpression() throws Exception {
        String dn = FileUtils.readFileToString(new File(getResourcePath("mtls/cel-test.txt")), "UTF-8");

        Map<String, Object> params = AuthenticationProviderMTls.parseDN(dn);

        ExpressionCompiler compiler = new ExpressionCompiler("DN.contains(\"CN=streamnative.io\")");
        Boolean eval = compiler.eval(params);
        Assert.assertTrue(eval);

        compiler = new ExpressionCompiler("O.contains(\"StreamNative\")");
        eval = compiler.eval(params);
        Assert.assertTrue(eval);

        compiler = new ExpressionCompiler("O==r\"StreamNative\\, Inc.\"");
        eval = compiler.eval(params);
        Assert.assertTrue(eval);

        System.out.println(params.get("ST"));
        compiler = new ExpressionCompiler("ST==r\"California\\0DAfter\"");
        eval = compiler.eval(params);
        Assert.assertTrue(eval);
    }

    @Test(dataProvider = "reuseMetadata")
    public void testAuthenticationProviderMTls(boolean reuseMetadata) throws Exception {
        AuthenticationProviderMTls authenticationProvider = new AuthenticationProviderMTls();
        if (reuseMetadata) {
            authenticationProvider.initialize(this.serviceConfiguration);
        } else {
            authenticationProvider.initialize(this.metadataStore);
        }

        String poolName = "test-pool";

        String sha1 = "c6deb54faffe854191ccb33ae44b9471c3a849a8".toUpperCase();
        String serialNumber = "61E61B07906A4FF7CD46B9591D3E1C390DF25E01";

        Pool pool = new Pool(poolName, Pool.AUTH_TYPE_MTLS, "this a test mtls type pool", "",
            "DN.contains(\"C=US\") && OU=='Apache Pulsar' && SAN.contains(\"IP:127.0.0.1\") && SHA1=='" + sha1 + "' && "
                + "SNID=='" + serialNumber + "'");

        authenticationProvider.getPoolResources().createPool(pool);

        Awaitility.await().until(() -> authenticationProvider.getPoolMap().size() == 1);

        X509Certificate[] x509Certificates =
            SecurityUtility.loadCertificatesFromPemFile(getResourcePath("mtls/client-cert.pem"));

        SSLSession sslSession = new MockSSLSession(x509Certificates);
        AuthenticationDataCommand authData = new AuthenticationDataCommand("", LocalAddress.ANY, sslSession);
        String principal = authenticationProvider.authenticate(authData);
        Assert.assertEquals(principal, poolName);
        authenticationProvider.close();
    }

    private String getResourcePath(String path) {
        // get resource directory path
        URL resource = this.getClass().getClassLoader().getResource(path);
        if (resource == null) {
            throw new RuntimeException("Resource not found: " + path);
        }
        return resource.getPath();
    }

    static class MockSSLSession implements SSLSession {
        private final Certificate[] peerCertificates;

        public MockSSLSession(Certificate[] peerCertificates) {
            this.peerCertificates = peerCertificates;
        }

        @Override
        public byte[] getId() {
            return new byte[0];
        }

        @Override
        public SSLSessionContext getSessionContext() {
            return null;
        }

        @Override
        public long getCreationTime() {
            return 0;
        }

        @Override
        public long getLastAccessedTime() {
            return 0;
        }

        @Override
        public void invalidate() {

        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public void putValue(String name, Object value) {

        }

        @Override
        public Object getValue(String name) {
            return null;
        }

        @Override
        public void removeValue(String name) {

        }

        @Override
        public String[] getValueNames() {
            return new String[0];
        }

        @Override
        public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
            return peerCertificates;
        }

        @Override
        public Certificate[] getLocalCertificates() {
            return new Certificate[0];
        }

        @Override
        public Principal getPeerPrincipal() throws SSLPeerUnverifiedException {
            return null;
        }

        @Override
        public Principal getLocalPrincipal() {
            return null;
        }

        @Override
        public String getCipherSuite() {
            return "";
        }

        @Override
        public String getProtocol() {
            return "";
        }

        @Override
        public String getPeerHost() {
            return "";
        }

        @Override
        public int getPeerPort() {
            return 0;
        }

        @Override
        public int getPacketBufferSize() {
            return 0;
        }

        @Override
        public int getApplicationBufferSize() {
            return 0;
        }
    }
}
