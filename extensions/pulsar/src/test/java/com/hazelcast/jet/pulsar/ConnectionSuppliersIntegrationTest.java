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

import com.hazelcast.test.SerialTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.apache.pulsar.shade.org.apache.pulsar.client.impl.PulsarClientImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.pulsar.ConnectionSuppliers.createAdmin;
import static com.hazelcast.jet.pulsar.ConnectionSuppliers.createClient;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SerialTest
@QuickTest
@ParallelJVMTest
@Timeout(60)
class ConnectionSuppliersIntegrationTest extends PulsarTestSupport {

    @ParameterizedTest
    @ValueSource(strings = {"", "pulsar.client.", "pulsar."})
    void createClient_loadsOptionsAndConnectsToBroker(String prefix) throws Exception {
        Map<String, String> options = Map.of(
                prefix + "numIoThreads", "2",
                prefix + "operationTimeoutMs", "15000",
                prefix + "tlsAllowInsecureConnection", "true");

        try (PulsarClient client = createClient(getServiceUrl(), false, options)) {
            var configuration = ((PulsarClientImpl) client).getConfiguration();
            assertThat(configuration.getServiceUrl()).isEqualTo(getServiceUrl());
            assertThat(configuration.getNumIoThreads()).isEqualTo(2);
            assertThat(configuration.getOperationTimeoutMs()).isEqualTo(15000);
            assertThat(configuration.isTlsAllowInsecureConnection()).isTrue();
            assertThat(configuration.isEnableTransaction()).isFalse();
            assertMessageRoundTrip(client, "persistent://public/default/" + randomName());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "pulsar.admin.", "pulsar."})
    void createAdmin_loadsOptionsAndConnectsToBroker(String prefix) throws Exception {
        String topic = "persistent://public/default/" + randomName();
        Map<String, String> options = Map.of(prefix + "serviceUrl", pulsarContainer.getHttpServiceUrl());

        // The configured URL must replace the unreachable argument when loadConf is applied.
        try (PulsarAdmin admin = createAdmin("http://127.0.0.1:1", options)) {
            assertThat(admin.getServiceUrl()).isEqualTo(pulsarContainer.getHttpServiceUrl());
            admin.topics().createPartitionedTopic(topic, 3);
            try {
                assertThat(admin.topics().getPartitionedTopicMetadata(topic).partitions).isEqualTo(3);
            } finally {
                admin.topics().deletePartitionedTopic(topic);
            }
        }
    }

    @Test
    void createConnections_acceptsEmptyOptions() throws Exception {
        String topic = "persistent://public/default/" + randomName();
        try (PulsarAdmin admin = createAdmin(pulsarContainer.getHttpServiceUrl(), Map.of());
             PulsarClient client = createClient(getServiceUrl(), false, Map.of())) {
            admin.topics().createNonPartitionedTopic(topic);
            try {
                assertMessageRoundTrip(client, topic);
            } finally {
                admin.topics().delete(topic);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void createClient_appliesTransactionSetting(boolean enableTransactions) throws Exception {
        try (PulsarClient client = createClient(getServiceUrl(), enableTransactions, Map.of())) {
            if (enableTransactions) {
                var transaction = client.newTransaction()
                                        .withTransactionTimeout(30, SECONDS)
                                        .build().get(30, SECONDS);
                transaction.abort().get(30, SECONDS);
            } else {
                assertThatThrownBy(() -> client.newTransaction().build().get(30, SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(PulsarClientException.InvalidConfigurationException.class)
                        .hasRootCauseMessage("Transactions are not enabled");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "pulsar.client.", "pulsar."})
    void createClient_rejectsInvalidOptionValue(String prefix) {
        assertThatThrownBy(() -> {
            try (PulsarClient ignored = createClient(getServiceUrl(), false,
                    Map.of(prefix + "numIoThreads", "not-a-number"))) {
                // Close resources if invalid configuration is unexpectedly accepted.
            }
        }).isInstanceOf(RuntimeException.class)
          .hasRootCauseInstanceOf(InvalidFormatException.class)
          .rootCause()
          .hasMessageContaining("not-a-number")
          .hasMessageContaining("numIoThreads");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "pulsar.admin.", "pulsar."})
    void createAdmin_rejectsInvalidOptionValue(String prefix) {
        assertThatThrownBy(() -> {
            try (PulsarAdmin ignored = createAdmin(pulsarContainer.getHttpServiceUrl(),
                    Map.of(prefix + "requestTimeoutMs", "not-a-number"))) {
                // Close resources if invalid configuration is unexpectedly accepted.
            }
        }).isInstanceOf(RuntimeException.class)
          .hasRootCauseInstanceOf(InvalidFormatException.class)
          .rootCause()
          .hasMessageContaining("not-a-number")
          .hasMessageContaining("requestTimeoutMs");
    }

    private void assertMessageRoundTrip(PulsarClient client, String topic) throws PulsarClientException {
        try (var consumer = client.newConsumer(Schema.STRING).topic(topic).subscriptionName(randomName()).subscribe();
             var producer = client.newProducer(Schema.STRING).topic(topic).create()) {
            producer.send("connection-suppliers-message");
            var message = consumer.receive(10, SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getValue()).isEqualTo("connection-suppliers-message");
            consumer.acknowledge(message);
        }
    }
}
