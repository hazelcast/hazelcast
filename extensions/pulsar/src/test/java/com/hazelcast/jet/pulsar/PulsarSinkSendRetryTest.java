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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pulsar.impl.PulsarSinkConfig;
import com.hazelcast.jet.pulsar.impl.WritePulsarP;
import com.hazelcast.test.SerialTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.shade.io.netty.channel.EventLoopGroup;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sending can be duplicated by the sink if connection
 * is lost just before receiving ACK from server - then
 * client by default sends another request.
 * <p>
 * This tests breaks connection after first receipt is received, simulating connection drop before client receives ack (receipt)
 * from the server.
 */
@SerialTest
class PulsarSinkSendRetryTest extends PulsarTestSupport {
    private static final String RETRIED_ITEM = "retried-item";
    private static final String END_MARKER = "end-marker";

    @Test
    @Timeout(value = 2, unit = MINUTES)
    void lostSendReceiptIsRetriedBeforeSnapshotCommit() throws Exception {
        String topic = "persistent://public/ns1/" + randomName();
        try (var admin = PulsarAdmin.builder().serviceHttpUrl(pulsarContainer.getHttpServiceUrl()).build()) {
            admin.namespaces().createNamespace("public/ns1");
            admin.topics().createNonPartitionedTopic(topic);
            admin.topicPolicies().setDeduplicationStatus(topic, true);
        }

        var clientConfig = new ClientConfigurationData();
        clientConfig.setServiceUrl(getServiceUrl());
        clientConfig.setEnableTransaction(true);
        clientConfig.setConnectionsPerBroker(1);
        var receiptLoss = new ReceiptLoss();
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(1, false, Executors.defaultThreadFactory());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try (var connectionPool = new ConnectionPool(InstrumentProvider.NOOP, clientConfig, eventLoopGroup,
                () -> receiptLoss.newConnection(clientConfig, eventLoopGroup), scheduler);
             var client = new PulsarClientImpl(clientConfig, eventLoopGroup, connectionPool);
             var reader = getClient().newReader(Schema.STRING)
                                     .topic(topic)
                                     .startMessageId(MessageId.earliest)
                                     .create()) {
            var config = createConfig(topic, client);

            var sink = new WritePulsarP<>(config);
            var dc = PulsarDataConnection.pulsarDataConnectionConf("pulsar",
                                                                   pulsarContainer.getPulsarBrokerUrl(),
                                                                   pulsarContainer.getHttpServiceUrl(),
                                                                   true);
            try {
                var outbox = new TestOutbox(new int[0], 1);
                var conf = smallInstanceConfig();
                conf.addDataConnectionConfig(dc);
                HazelcastInstance instance = createHazelcastInstance(conf);
                sink.init(outbox, new TestProcessorContext()
                                      .setProcessingGuarantee(EXACTLY_ONCE)
                                      .setHazelcastInstance(instance));
                var inbox = new TestInbox(List.of(RETRIED_ITEM, END_MARKER));
                sink.process(0, inbox);
                assertThat(inbox.isEmpty()).isTrue();

                sink.saveToSnapshot();
                sink.snapshotCommitPrepare();
                sink.snapshotCommitFinish(true);

                List<String> actual = readThroughEndMarker(reader);
                List<String> expected = List.of(RETRIED_ITEM, END_MARKER);
                assertThat(actual).containsExactlyElementsOf(expected);
            } finally {
                sink.close();
            }
        } finally {
            scheduler.shutdownNow();
            eventLoopGroup.shutdownGracefully(0, 10, SECONDS).get(15, SECONDS);
        }
    }

    private static @NonNull PulsarSinkConfig<String, String> createConfig(String topic, PulsarClientImpl client) {
        var config = new PulsarSinkConfig<String, String>();
        config.setTopic(topic);
        config.setConnectionSupplier(() -> client);
        config.setDataConnectionRef(DataConnectionRef.dataConnectionRef("pulsar"));
        config.setSchemaSupplier(() -> Schema.STRING);
        config.setExtractValueFn(value -> value);
        config.setOptions(Map.of());
        config.setTransactional(true);
        config.setTransactionTimeout(Duration.ofMinutes(1));
        config.setProducerCustomizer(builder -> builder.enableBatching(false).sendTimeout(30, SECONDS));
        return config;
    }

    private static List<String> readThroughEndMarker(Reader<String> reader) throws Exception {
        List<String> result = new ArrayList<>();
        // At most three messages are expected. A fourth already proves an unexpected duplicate.
        for (int i = 0; i < 4; i++) {
            Message<String> message = reader.readNext(10, SECONDS);
            assertThat(message).as("committed output must reach the end marker").isNotNull();
            result.add(message.getValue());
            if (END_MARKER.equals(message.getValue())) {
                return result;
            }
        }
        throw new AssertionError("End marker missing from output: " + result);
    }

    private record Receipt(long producerId, long sequenceId, long ledgerId, long entryId, ClientCnx connection) {
        private Receipt(CommandSendReceipt receipt, ClientCnx connection) {
            this(receipt.getProducerId(), receipt.getSequenceId(), receipt.getMessageId().getLedgerId(),
                    receipt.getMessageId().getEntryId(), connection);
        }
    }

    private static final class ReceiptLoss {
        private final AtomicReference<Receipt> dropped = new AtomicReference<>();

        private ClientCnx newConnection(ClientConfigurationData config, EventLoopGroup eventLoopGroup) {
            return new ClientCnx(InstrumentProvider.NOOP, config, eventLoopGroup) {
                @Override
                protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
                    Receipt receipt = new Receipt(sendReceipt, this);
                    if (dropped.compareAndSet(null, receipt)) {
                        // drop connection after receiving a Receipt
                        ctx().close();
                        return;
                    }
                    super.handleSendReceipt(sendReceipt);
                }
            };
        }
    }
}
