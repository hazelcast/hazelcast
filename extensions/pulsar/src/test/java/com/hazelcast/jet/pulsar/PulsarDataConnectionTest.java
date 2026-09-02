/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.pulsar;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.pulsar.PulsarDataConnection.pulsarDataConnectionConf;
import static org.assertj.core.api.Assertions.assertThat;

@SerialTest
@QuickTest
@ParallelJVMTest
public class PulsarDataConnectionTest extends PulsarTestSupport {
    private PulsarDataConnection dataConnection;
    private String brokerUrl;
    private String serviceUrl;
    private PulsarAdmin pulsarAdmin;

    @BeforeEach
    public void setup() throws PulsarClientException {
        this.brokerUrl = pulsarContainer.getPulsarBrokerUrl();
        this.serviceUrl = pulsarContainer.getHttpServiceUrl();
        pulsarAdmin = PulsarAdmin.builder()
                          .serviceHttpUrl(this.serviceUrl)
                          .build();
    }

    @AfterEach
    public void cleanup() {
        if (dataConnection != null) {
            dataConnection.destroy();
            dataConnection = null;
        }
    }

    @Test
    public void should_return_same_link_when_shared() {
        dataConnection = new PulsarDataConnection(newConnection());

        PulsarClient client1 = dataConnection.getClient();
        PulsarClient client2 = dataConnection.getClient();

        assertThat(client1).isNotNull();
        assertThat(client2).isNotNull();
        assertThat(client1).isSameAs(client2);
    }

    @Test
    public void should_close_client_when_all_released() throws PulsarClientException {
        dataConnection = new PulsarDataConnection(newConnection());

        PulsarClient client1 = dataConnection.getClient();
        PulsarClient client2 = dataConnection.getClient();

        client1.close();
        assertNotClosed(client2);
        client2.close();

        dataConnection.release();

        assertClosed(client1);
        assertClosed(client2);
    }

    @Test
    public void should_closeAsync_client_when_all_released() throws ExecutionException, InterruptedException {
        dataConnection = new PulsarDataConnection(newConnection());

        PulsarClient client1 = dataConnection.getClient();
        PulsarClient client2 = dataConnection.getClient();

        client1.closeAsync().get();
        assertNotClosed(client2);
        client2.closeAsync().get();

        dataConnection.release();

        assertClosed(client1);
        assertClosed(client2);
    }

    @Test
    public void should_shutdown_client_when_all_released() throws PulsarClientException {
        dataConnection = new PulsarDataConnection(newConnection());

        PulsarClient client1 = dataConnection.getClient();
        PulsarClient client2 = dataConnection.getClient();

        client1.shutdown();
        assertNotClosed(client2);
        client2.shutdown();

        dataConnection.release();

        assertClosed(client1);
        assertClosed(client2);
    }

    @Test
    public void should_return_resource_types() {
        // given
        dataConnection = new PulsarDataConnection(newConnection());

        // when
        Collection<String> resourcedTypes = dataConnection.resourceTypes();

        //then
        assertThat(resourcedTypes)
                .map(r -> r.toLowerCase(Locale.ROOT))
                .containsExactlyInAnyOrder("topic");
    }

    @Test
    public void should_return_collections_when_listResources() throws PulsarAdminException {
        pulsarAdmin.clusters().createCluster("cluster1", ClusterData.builder()
                                                                    .brokerServiceUrl(brokerUrl)
                                                                    .serviceUrl(serviceUrl)
                                                                    .build());
        pulsarAdmin.topics().createPartitionedTopic("partitionedTopic1", 2);
        pulsarAdmin.tenants().createTenant("testTenant", TenantInfo.builder()
                                                                   .allowedClusters(Set.of("cluster1", "standalone"))
                                                                   .build());
        pulsarAdmin.namespaces().createNamespace("testTenant/testNamespace");
        pulsarAdmin.namespaces().createNamespace("testTenant/testNamespaceOther");
        pulsarAdmin.topics().createPartitionedTopic("testTenant/testNamespace/topicUnderTN1", 2);
        pulsarAdmin.topics().createPartitionedTopic("testTenant/testNamespaceOther/topicUnderTN2", 2);

        dataConnection = new PulsarDataConnection(newConnection());
        assertThat(dataConnection.listResources()).contains(
            new DataConnectionResource("Topic", "public", "default", "partitionedTopic1"),
            new DataConnectionResource("Topic", "testTenant", "testNamespace", "topicUnderTN1"),
            new DataConnectionResource("Topic", "testTenant", "testNamespaceOther", "topicUnderTN2")
        );
        dataConnection.destroy();

        DataConnectionConfig config = newConnection();
        config.setProperty("tenant", "testTenant");
        dataConnection = new PulsarDataConnection(config);
        assertThat(dataConnection.listResources()).contains(
            new DataConnectionResource("Topic", "testTenant", "testNamespace", "topicUnderTN1"),
            new DataConnectionResource("Topic", "testTenant", "testNamespaceOther", "topicUnderTN2")
        );
        dataConnection.destroy();

        config = newConnection();
        config.setProperty("tenant", "testTenant");
        config.setProperty("namespace", "testNamespace");
        dataConnection = new PulsarDataConnection(config);
        assertThat(dataConnection.listResources()).contains(
                new DataConnectionResource("Topic", "testTenant", "testNamespace", "topicUnderTN1")

        );
        dataConnection.destroy();

        config = newConnection();
        config.setProperty("tenant", "testTenant");
        config.setProperty("namespace", "testTenant/testNamespace");
        dataConnection = new PulsarDataConnection(config);
        assertThat(dataConnection.listResources()).contains(
                new DataConnectionResource("Topic", "testTenant", "testNamespace", "topicUnderTN1")
        );
        dataConnection.destroy();
    }

    @Test
    public void should_return_new_link_when_not_shared() {
        dataConnection = new PulsarDataConnection(pulsarDataConnectionConf("pulsar", brokerUrl, serviceUrl).setShared(false));

        PulsarClient client1 = dataConnection.getClient();
        PulsarClient client2 = dataConnection.getClient();

        assertThat(client1).isNotNull();
        assertThat(client2).isNotNull();
        assertThat(client1).isNotSameAs(client2);
    }


    @Test
    public void should_close_client_when_all_released_when_not_shared() throws PulsarClientException {
        dataConnection = new PulsarDataConnection(newConnection().setShared(false));

        PulsarClient client1 = dataConnection.getClient();
        PulsarClient client2 = dataConnection.getClient();

        client1.close();
        assertNotClosed(client2);
        client2.close();

        assertClosed(client1);
        assertClosed(client2);
    }

    @Test
    public void should_closeAsync_client_when_all_released_when_not_shared() throws ExecutionException, InterruptedException {
        dataConnection = new PulsarDataConnection(newConnection().setShared(false));

        PulsarClient client1 = dataConnection.getClient();
        PulsarClient client2 = dataConnection.getClient();

        client1.closeAsync().get();
        assertNotClosed(client2);
        client2.closeAsync().get();

        assertClosed(client1);
        assertClosed(client2);
    }

    private @NonNull DataConnectionConfig newConnection() {
        return pulsarDataConnectionConf("pulsar", brokerUrl, serviceUrl);
    }

    private void assertNotClosed(PulsarClient client) {
        assertThat(client.isClosed()).isFalse();
    }
    private void assertClosed(PulsarClient client) {
        assertThat(client.isClosed()).isTrue();
    }

}
