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
package io.streamnative.pulsar.handlers.mqtt.base;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Base test class for MQTT Client.
 */
@Slf4j
public class MQTTTestBase extends MQTTProtocolHandlerTestBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");
        ClusterData clusterData = ClusterData.builder()
                .serviceUrl("http://127.0.0.1:" + getBrokerWebservicePortList().get(0))
                .build();
        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName, clusterData);
        } else {
            admin.clusters().updateCluster(configClusterName, clusterData);
        }
        TenantInfo tenantInfo = TenantInfo.builder()
                .adminRoles(Sets.newHashSet("appid1", "appid2"))
                .allowedClusters(Sets.newHashSet("test"))
                .build();
        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public", tenantInfo);
        } else {
            admin.tenants().updateTenant("public", tenantInfo);
        }

        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setRetention("public/default",
                    new RetentionPolicies(60, 1000));
        }

        checkPulsarServiceState();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }
}
