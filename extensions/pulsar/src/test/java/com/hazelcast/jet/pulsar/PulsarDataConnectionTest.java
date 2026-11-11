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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.BROKER_URL;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.CLOSE_TIMEOUT_SECONDS;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.ENABLE_TRANSACTIONS;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.Properties.HTTP_SERVICE_URL;
import static com.hazelcast.jet.pulsar.PulsarDataConnection.pulsarDataConnectionConf;
import static com.hazelcast.jet.pulsar.PulsarSchema.bytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        pulsarAdmin.topics().createNonPartitionedTopic("testTenant/testNamespaceOther/nonPartitionedTopicUnderNS");
        pulsarAdmin.topics().createPartitionedTopic("non-persistent://testTenant/testNamespaceOther/nonPersistent", 2);

        dataConnection = new PulsarDataConnection(newConnection());
        assertThat(dataConnection.listResources()).contains(
            new DataConnectionResource("Topic", "persistent", "public", "default", "partitionedTopic1"),
            new DataConnectionResource("Topic", "persistent", "testTenant", "testNamespace", "topicUnderTN1"),
            new DataConnectionResource("Topic", "persistent", "testTenant", "testNamespaceOther", "topicUnderTN2"),
            new DataConnectionResource("Topic", "persistent", "testTenant", "testNamespaceOther", "nonPartitionedTopicUnderNS"),
            new DataConnectionResource("Topic", "non-persistent", "testTenant", "testNamespaceOther", "nonPersistent")
        );
        dataConnection.destroy();

        DataConnectionConfig config = newConnection();
        config.setProperty("tenant", "testTenant");
        dataConnection = new PulsarDataConnection(config);
        assertThat(dataConnection.listResources()).contains(
            new DataConnectionResource("Topic", "persistent", "testTenant", "testNamespace", "topicUnderTN1"),
            new DataConnectionResource("Topic", "persistent", "testTenant", "testNamespaceOther", "topicUnderTN2")
        );
        dataConnection.destroy();

        config = newConnection();
        config.setProperty("tenant", "testTenant");
        config.setProperty("namespace", "testNamespace");
        dataConnection = new PulsarDataConnection(config);
        assertThat(dataConnection.listResources()).contains(
                new DataConnectionResource("Topic", "persistent", "testTenant", "testNamespace", "topicUnderTN1")

        );
        dataConnection.destroy();

        config = newConnection();
        config.setProperty("tenant", "testTenant");
        config.setProperty("namespace", "testTenant/testNamespace");
        dataConnection = new PulsarDataConnection(config);
        assertThat(dataConnection.listResources()).contains(
                new DataConnectionResource("Topic", "persistent", "testTenant", "testNamespace", "topicUnderTN1")
        );
        dataConnection.destroy();
    }

    @Test
    public void should_throw_when_getClient_called_after_destroy() {
        dataConnection = new PulsarDataConnection(newConnection());
        dataConnection.getClient();
        dataConnection.destroy();

        assertThatThrownBy(() -> dataConnection.getClient())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Pulsar client should not be closed at this point");
    }

    @Test
    public void should_not_throw_when_destroy_called_before_any_getClient() {
        dataConnection = new PulsarDataConnection(newConnection());

        assertThatCode(dataConnection::destroy).doesNotThrowAnyException();
    }

    @Test
    public void should_throw_when_brokerUrl_is_malformed() {
        dataConnection = new PulsarDataConnection(pulsarDataConnectionConf("pulsar", "not-a-valid-broker-url", serviceUrl));

        assertThatThrownBy(() -> dataConnection.getClient())
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("Error while trying to create PulsarClient");
    }

    @Test
    public void should_throw_when_closeTimeoutSeconds_is_not_a_number() {
        DataConnectionConfig config = newConnection();
        config.setProperty(CLOSE_TIMEOUT_SECONDS, "not-a-number");

        // propagates straight from Long.parseLong() - NumberFormatException is itself an
        // IllegalArgumentException, so no custom wrapping is needed
        assertThatThrownBy(() -> new PulsarDataConnection(config))
                .isInstanceOf(NumberFormatException.class)
                .hasMessageContaining("not-a-number");
    }

    @Test
    public void should_throw_when_closeTimeoutSeconds_is_not_positive() {
        DataConnectionConfig zeroConfig = newConnection();
        zeroConfig.setProperty(CLOSE_TIMEOUT_SECONDS, "0");
        assertThatThrownBy(() -> new PulsarDataConnection(zeroConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closeTimeoutSeconds is 0 but must be > 0");

        DataConnectionConfig negativeConfig = newConnection();
        negativeConfig.setProperty(CLOSE_TIMEOUT_SECONDS, "-5");
        assertThatThrownBy(() -> new PulsarDataConnection(negativeConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closeTimeoutSeconds is -5 but must be > 0");
    }

    @Test
    public void should_fallback_to_shutdown_within_configured_closeTimeoutSeconds() {
        // a syntactically valid but non-routable broker address (TEST-NET-1, RFC 5737) - the
        // client constructs fine (serviceUrl validation doesn't require connectivity) but can
        // never actually close gracefully, so destroy() must fall back to shutdown() once the
        // configured timeout elapses, rather than hang
        DataConnectionConfig config = newConnection();
        config.setProperty(BROKER_URL, "pulsar://192.0.2.1:6650");
        config.setProperty(CLOSE_TIMEOUT_SECONDS, "1");
        dataConnection = new PulsarDataConnection(config);
        dataConnection.getClient();

        long startNanos = System.nanoTime();
        assertThatCode(dataConnection::destroy).doesNotThrowAnyException();
        long elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000;

        assertThat(elapsedSeconds)
                .as("destroy() must fall back to shutdown() close to the configured 1-second "
                        + "closeTimeoutSeconds, not hang for the default 5 minutes")
                .isLessThan(30);
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

    /**
     * Unlike {@code close()}/{@code closeAsync()}, the real {@code PulsarClient.shutdown()} does
     * not flip {@code isClosed()} to true (confirmed empirically against the 5.0.1 client) - so
     * this only asserts shutdown() completes safely for both independently-created clients,
     * rather than mirroring the close()/closeAsync() tests' isClosed() assertions.
     */
    @Test
    public void should_shutdown_both_clients_without_error_when_not_shared() {
        dataConnection = new PulsarDataConnection(newConnection().setShared(false));

        PulsarClient client1 = dataConnection.getClient();
        PulsarClient client2 = dataConnection.getClient();

        assertThatCode(client1::shutdown).doesNotThrowAnyException();
        assertThatCode(client2::shutdown).doesNotThrowAnyException();
    }

    @Test
    public void should_throw_when_brokerUrl_missing() {
        DataConnectionConfig config = new DataConnectionConfig()
                .setName("pulsar")
                .setType("Pulsar")
                .setShared(true);
        config.setProperty(HTTP_SERVICE_URL, serviceUrl);

        assertThatThrownBy(() -> new PulsarDataConnection(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Pulsar Broker URL is required");
    }

    @Test
    public void should_throw_when_httpServiceUrl_missing() {
        DataConnectionConfig config = new DataConnectionConfig()
                .setName("pulsar")
                .setType("Pulsar")
                .setShared(true);
        config.setProperty(BROKER_URL, brokerUrl);

        assertThatThrownBy(() -> new PulsarDataConnection(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Pulsar HTTP Service URL is required");
    }

    @Test
    public void should_return_empty_list_when_namespace_unknown() {
        DataConnectionConfig config = newConnection();
        config.setProperty("namespace", "thisNamespaceDoesNotExist");
        dataConnection = new PulsarDataConnection(config);

        assertThat(dataConnection.listResources()).isEmpty();
    }

    @Test
    public void should_throw_when_listResources_called_with_unreachable_httpServiceUrl() {
        dataConnection = new PulsarDataConnection(pulsarDataConnectionConf("pulsar", brokerUrl, "http://127.0.0.1:1"));

        assertThatThrownBy(() -> dataConnection.listResources())
                .isInstanceOf(com.hazelcast.core.HazelcastException.class);
    }

    @Test
    public void should_create_transactional_client_when_enableTransactions_true() throws Exception {
        DataConnectionConfig config = newConnection();
        config.setProperty(ENABLE_TRANSACTIONS, "true");
        dataConnection = new PulsarDataConnection(config);

        PulsarClient client = dataConnection.getClient();
        Transaction txn = client.newTransaction()
                                 .withTransactionTimeout(5, TimeUnit.SECONDS)
                                 .build()
                                 .get();
        assertThat(txn).isNotNull();
        txn.abort().get();
    }

    @Test
    public void should_fail_to_create_transaction_when_enableTransactions_false() {
        dataConnection = new PulsarDataConnection(newConnection());

        PulsarClient client = dataConnection.getClient();
        assertThatThrownBy(() -> client.newTransaction()
                                        .withTransactionTimeout(5, TimeUnit.SECONDS)
                                        .build()
                                        .get())
                .isInstanceOf(Exception.class);
    }

    @Test
    public void destroy_should_be_idempotent() {
        dataConnection = new PulsarDataConnection(newConnection());
        dataConnection.getClient();

        dataConnection.destroy();
        assertThatCode(dataConnection::destroy).doesNotThrowAnyException();
    }

    @Test
    void schema_serializability_checked_after_assignment() {
        // we expect no fail
        assertThatCode(() -> PulsarSources.pulsarConsumerBuilder(bytes())).doesNotThrowAnyException();
        assertThatCode(() -> PulsarSources.pulsarReaderBuilder(bytes())).doesNotThrowAnyException();
        assertThatCode(() -> PulsarSinks.builder(bytes())).doesNotThrowAnyException();
    }

    private DataConnectionConfig newConnection() {
        return pulsarDataConnectionConf("pulsar", brokerUrl, serviceUrl);
    }

    private void assertNotClosed(PulsarClient client) {
        assertThat(client.isClosed()).isFalse();
    }
    private void assertClosed(PulsarClient client) {
        assertThat(client.isClosed()).isTrue();
    }

}
