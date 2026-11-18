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
package com.hazelcast.jet.pulsar.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pulsar.PulsarSinkBuilder;
import com.hazelcast.test.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.Serial;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.impl.util.JetExceptionUtil.isRestartableException;
import static com.hazelcast.test.HazelcastTestSupport.getFieldValue;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuickTest
@ParallelTest
@Timeout(30)
class WritePulsarPTest {

    private static final int IN_FLIGHT_LIMIT = EdgeConfig.DEFAULT_QUEUE_SIZE;
    private final List<CompletableFuture<MessageId>> sendFutures = new CopyOnWriteArrayList<>();
    private PulsarClient client;
    private PulsarSinkConfig<String, String> config;
    private TypedMessageBuilder<String> messageBuilder;
    private WritePulsarP<String, String> processor;
    private ExecutorService executor;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        client = mock(PulsarClient.class);
        ProducerBuilder<String> producerBuilder = mock(ProducerBuilder.class, RETURNS_SELF);
        Producer<String> producer = mock(Producer.class);
        messageBuilder = mock(TypedMessageBuilder.class, RETURNS_SELF);
        when(client.newProducer(Schema.STRING)).thenReturn(producerBuilder);
        when(producerBuilder.create()).thenReturn(producer);
        when(producer.newMessage()).thenReturn(messageBuilder);
        when(messageBuilder.sendAsync()).thenAnswer(invocation -> {
            var future = new CompletableFuture<MessageId>();
            sendFutures.add(future);
            return future;
        });

        config = new PulsarSinkConfig<>();
        config.setTopic("backpressure-test");
        config.setConnectionSupplier(() -> client);
        config.setSchemaSupplier(() -> Schema.STRING);
        config.setExtractValueFn(value -> value);
        config.setOptions(Map.of());
        config.setTransactional(false);
        initProcessor();
    }

    private void initProcessor() throws Exception {
        processor = new WritePulsarP<>(config, client);
        processor.init(new TestOutbox(new int[0], 1), new TestProcessorContext().setProcessingGuarantee(AT_LEAST_ONCE));
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdownNow();
        try {
            assertThat(executor.awaitTermination(5, SECONDS)).as("processor thread must terminate").isTrue();
        } finally {
            if (processor != null) {
                processor.close();
            }
        }
    }

    @Test
    void whenInFlightLimitReached_thenLeavesRemainingItemsInInbox() throws Exception {
        List<String> input = input(IN_FLIGHT_LIMIT + 1);
        var inbox = new TestInbox(input);

        process(inbox).get(5, SECONDS);

        assertThat(sendFutures).hasSize(IN_FLIGHT_LIMIT);
        assertThat(inbox).containsExactly(input.get(IN_FLIGHT_LIMIT));
        assertSentExactly(input.subList(0, IN_FLIGHT_LIMIT));

        process(inbox).get(5, SECONDS);

        assertThat(sendFutures).hasSize(IN_FLIGHT_LIMIT);
        assertThat(inbox).containsExactly(input.get(IN_FLIGHT_LIMIT));
        assertSentExactly(input.subList(0, IN_FLIGHT_LIMIT));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    @SuppressWarnings("unchecked")
    void whenCustomInFlightLimitSetOnBuilder_thenUsesFixedProcessorLimit(int maxInFlightMessages) throws Exception {
        var builder = new PulsarSinkBuilder<>(() -> Schema.STRING, FunctionEx.identity())
                .topic("custom-backpressure-test")
                .connectionSupplier(new TestClientSupplier(client))
                .useTransactions(false)
                .maxInFlightMessages(maxInFlightMessages);
        var sink = (SinkImpl<String>) builder.build();
        builder.maxInFlightMessages(maxInFlightMessages + 1);
        var context = new TestProcessorContext().setProcessingGuarantee(AT_LEAST_ONCE);
        var metaSupplier = sink.metaSupplier();
        metaSupplier.init(context);
        var address = new Address("127.0.0.1", 5701);
        var supplier = metaSupplier.get(List.of(address)).apply(address);
        supplier.init(context);
        processor.close();
        processor = (WritePulsarP<String, String>) supplier.get(1).iterator().next();
        processor.init(new TestOutbox(new int[0], 1), context);
        List<String> input = input(maxInFlightMessages + 1);
        var inbox = new TestInbox(input);

        process(inbox).get(5, SECONDS);

        assertThat(sendFutures).hasSize(maxInFlightMessages);
        assertThat(inbox).containsExactly(input.get(maxInFlightMessages));
        assertSentExactly(input.subList(0, maxInFlightMessages));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
    void whenInFlightLimitIsNotPositive_thenBuilderRejectsIt(int maxInFlightMessages) {
        var builder = new PulsarSinkBuilder<>(() -> Schema.STRING, FunctionEx.identity());

        assertThatThrownBy(() -> builder.maxInFlightMessages(maxInFlightMessages))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxInFlightMessages");
    }

    @Test
    void whenAcknowledgementsArrive_thenResumesWithoutLosingOrDuplicatingItems() throws Exception {
        List<String> input = input(IN_FLIGHT_LIMIT + 2);
        var inbox = new TestInbox(input);
        process(inbox).get(5, SECONDS);
        assertThat(sendFutures).hasSize(IN_FLIGHT_LIMIT);
        assertThat(inbox).containsExactlyElementsOf(input.subList(IN_FLIGHT_LIMIT, input.size()));

        sendFutures.forEach(future -> future.complete(MessageId.latest));
        doReturn(CompletableFuture.completedFuture(MessageId.latest)).when(messageBuilder).sendAsync();

        process(inbox).get(5, SECONDS);

        assertThat(inbox).isEmpty();
        assertSentExactly(input);
    }

    @Test
    void whenSendsAlreadyAcknowledged_thenProcessesMoreThanInFlightLimit() throws Exception {
        doReturn(CompletableFuture.completedFuture(MessageId.latest)).when(messageBuilder).sendAsync();
        List<String> input = input(IN_FLIGHT_LIMIT + 2);
        var inbox = new TestInbox(input);

        process(inbox).get(5, SECONDS);

        assertThat(inbox).isEmpty();
        assertSentExactly(input);
        AtomicInteger inFlightMessages = getFieldValue(processor, "inFlightMessages");
        assertThat(inFlightMessages.get()).isZero();
    }

    @Test
    void whenOneAcknowledgementArrives_thenSendsOnlyOneMoreItem() throws Exception {
        List<String> input = input(IN_FLIGHT_LIMIT + 2);
        var inbox = new TestInbox(input);
        process(inbox).get(5, SECONDS);

        sendFutures.get(1).complete(MessageId.latest);
        process(inbox).get(5, SECONDS);

        assertThat(sendFutures).hasSize(IN_FLIGHT_LIMIT + 1);
        assertThat(inbox).containsExactly(input.get(IN_FLIGHT_LIMIT + 1));
        assertSentExactly(input.subList(0, IN_FLIGHT_LIMIT + 1));

        process(inbox).get(5, SECONDS);

        assertThat(sendFutures).hasSize(IN_FLIGHT_LIMIT + 1);
        assertThat(inbox).containsExactly(input.get(IN_FLIGHT_LIMIT + 1));
    }

    @Test
    void whenAcknowledgementsArriveConcurrently_thenDoesNotLoseOrDuplicateItems() throws Exception {
        ExecutorService acknowledger = Executors.newSingleThreadExecutor();
        try {
            when(messageBuilder.sendAsync()).thenAnswer(invocation -> {
                var future = new CompletableFuture<MessageId>();
                sendFutures.add(future);
                acknowledger.submit(() -> future.complete(MessageId.latest));
                return future;
            });
            List<String> input = input(2 * IN_FLIGHT_LIMIT);
            var inbox = new TestInbox(input);

            while (!inbox.isEmpty()) {
                process(inbox).get(5, SECONDS);
            }
            acknowledger.shutdown();
            assertThat(acknowledger.awaitTermination(5, SECONDS)).as("all acknowledgements must finish").isTrue();

            assertSentExactly(input);
            assertThat(processor.complete()).isTrue();
        } finally {
            acknowledger.shutdownNow();
            assertThat(acknowledger.awaitTermination(5, SECONDS)).as("acknowledger thread must terminate").isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void whenSendCompletes_thenReleasesItsSlot(boolean successful) throws Exception {
        process(new TestInbox(input(1))).get(5, SECONDS);
        AtomicInteger inFlightMessages = getFieldValue(processor, "inFlightMessages");
        assertThat(inFlightMessages.get()).isEqualTo(1);

        if (successful) {
            sendFutures.get(0).complete(MessageId.latest);
        } else {
            sendFutures.get(0).completeExceptionally(new PulsarClientException.TimeoutException("send timed out"));
        }

        assertThat(inFlightMessages.get()).isZero();
    }

    @Test
    void whenSendsArePending_thenSnapshotWaitsForEveryAcknowledgement() throws Exception {
        process(new TestInbox(input(2))).get(5, SECONDS);

        assertThat(processor.snapshotCommitPrepare()).isFalse();
        sendFutures.get(1).complete(MessageId.latest);
        assertThat(processor.snapshotCommitPrepare()).isFalse();
        sendFutures.get(0).complete(MessageId.latest);

        assertThat(processor.snapshotCommitPrepare()).isTrue();
        assertThat(processor.snapshotCommitFinish(true)).isTrue();
    }

    @Test
    void whenSendFailsAfterProcessing_thenSnapshotFails() throws Exception {
        process(new TestInbox(input(1))).get(5, SECONDS);
        var failure = new PulsarClientException.TimeoutException("send timed out");
        sendFutures.get(0).completeExceptionally(failure);

        assertThatThrownBy(() -> processor.snapshotCommitPrepare()).hasCause(failure);
    }

    @Test
    void whenSendFailsWhileInputIsIdle_thenTryProcessFails() throws Exception {
        process(new TestInbox(input(1))).get(5, SECONDS);
        var failure = new PulsarClientException.TimeoutException("send timed out");
        sendFutures.get(0).completeExceptionally(failure);

        assertThatThrownBy(() -> processor.tryProcess()).hasCause(failure);
    }

    @Test
    void whenSendFailsWithOtherSendsPending_thenLifecycleMethodsFail() throws Exception {
        process(new TestInbox(input(2))).get(5, SECONDS);
        var failure = new PulsarClientException.TimeoutException("send timed out");
        sendFutures.get(0).completeExceptionally(failure);

        assertThatThrownBy(() -> processor.tryProcess()).hasCause(failure);
        assertThatThrownBy(() -> processor.snapshotCommitPrepare()).hasCause(failure);
        assertThatThrownBy(() -> processor.complete()).hasCause(failure);
    }

    @Test
    void whenSendsArePending_thenCompleteWaitsForEveryAcknowledgement() throws Exception {
        process(new TestInbox(input(2))).get(5, SECONDS);

        assertThat(processor.complete()).isFalse();
        sendFutures.get(1).complete(MessageId.latest);
        assertThat(processor.complete()).isFalse();
        sendFutures.get(0).complete(MessageId.latest);

        assertThat(processor.complete()).isTrue();
    }

    @Test
    void whenLastPendingSendFails_thenCompleteFails() throws Exception {
        process(new TestInbox(input(2))).get(5, SECONDS);
        sendFutures.get(0).complete(MessageId.latest);
        assertThat(processor.complete()).isFalse();
        var failure = new PulsarClientException.TimeoutException("send timed out");
        sendFutures.get(1).completeExceptionally(failure);

        assertThatThrownBy(() -> processor.complete()).hasCause(failure);
    }

    @Test
    void whenSendFailsWhileBackpressured_thenFailsBeforeSendingMoreItems() throws Exception {
        List<String> input = input(IN_FLIGHT_LIMIT + 1);
        var inbox = new TestInbox(input);
        process(inbox).get(5, SECONDS);
        var failure = new PulsarClientException.TimeoutException("send timed out");
        sendFutures.get(0).completeExceptionally(failure);

        assertThatThrownBy(() -> process(inbox).get(5, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCauseInstanceOf(PulsarClientException.TimeoutException.class);
        assertThat(inbox).containsExactly(input.get(IN_FLIGHT_LIMIT));
        assertSentExactly(input.subList(0, IN_FLIGHT_LIMIT));
    }

    @ParameterizedTest
    @ValueSource(strings = {"value", "key", "keyAndValue", "properties", "timestamp"})
    void whenMappingThrowsNearInFlightLimit_thenDoesNotCountUnsentItemAndRetainsFailure(String extractor) throws Exception {
        var failure = new IllegalArgumentException("mapping failed");
        FunctionEx<String, String> mappingFn = item -> {
            if (item.equals("fail")) {
                throw failure;
            }
            return item;
        };
        switch (extractor) {
            case "value" -> config.setExtractValueFn(mappingFn);
            case "key" -> config.setExtractKeyFn(mappingFn);
            case "keyAndValue" -> config.setExtractKeyAndValueFn(item -> Map.entry("key", mappingFn.apply(item)));
            case "properties" -> config.setExtractPropertiesFn(item -> Map.of("property", mappingFn.apply(item)));
            case "timestamp" -> config.setExtractTimestampFn(item -> Long.valueOf(mappingFn.apply(item)));
            default -> throw new AssertionError("Unknown extractor: " + extractor);
        }
        processor.close();
        initProcessor();
        AtomicInteger inFlightMessages = getFieldValue(processor, "inFlightMessages");
        List<String> sent = input(IN_FLIGHT_LIMIT - 1);
        process(new TestInbox(sent)).get(5, SECONDS);
        assertThat(inFlightMessages.get()).isEqualTo(IN_FLIGHT_LIMIT - 1);
        var inbox = new TestInbox(List.of("fail", "remaining"));

        assertThatThrownBy(() -> process(inbox).get(5, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCause(failure);

        assertThat(inFlightMessages.get()).isEqualTo(IN_FLIGHT_LIMIT - 1);
        assertThat(inbox).containsExactly("fail", "remaining");
        assertSentExactly(sent);
        sendFutures.forEach(future -> future.complete(MessageId.latest));
        assertThat(inFlightMessages.get()).isZero();
        assertThatThrownBy(() -> process(inbox).get(5, SECONDS)).hasRootCause(failure);
        assertThat(inbox).containsExactly("fail", "remaining");
        assertSentExactly(sent);
    }

    @Test
    void whenSendThrowsSynchronously_thenDoesNotCountUnsentItemAndRetainsFailure() {
        var failure = new IllegalArgumentException("send failed");
        doThrow(failure).when(messageBuilder).sendAsync();
        var inbox = new TestInbox(List.of("fail", "remaining"));

        assertThatThrownBy(() -> process(inbox).get(5, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasRootCause(failure);
        AtomicInteger inFlightMessages = getFieldValue(processor, "inFlightMessages");
        assertThat(inFlightMessages.get()).isZero();
        assertThat(inbox).containsExactly("fail", "remaining");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void whenSendFailsImmediately_thenJobCanRestartRegardlessOfInboxSize(int inboxSize) {
        var failure = new PulsarClientException.TimeoutException("send timed out");
        doReturn(CompletableFuture.failedFuture(failure)).when(messageBuilder).sendAsync();

        Throwable thrown = catchThrowable(() -> process(new TestInbox(input(inboxSize))).get(5, SECONDS));

        assertThat(thrown).isInstanceOf(ExecutionException.class)
                          .hasRootCauseInstanceOf(PulsarClientException.TimeoutException.class);
        assertThat(isRestartableException(thrown.getCause())).as("retryable send failure must restart the job").isTrue();
        verify(messageBuilder).sendAsync();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void whenSendFailsPermanently_thenJobFailsWithoutRestartRegardlessOfInboxSize(int inboxSize) {
        var failure = new PulsarClientException.InvalidMessageException("message cannot be sent");
        doReturn(CompletableFuture.failedFuture(failure)).when(messageBuilder).sendAsync();

        Throwable thrown = catchThrowable(() -> process(new TestInbox(input(inboxSize))).get(5, SECONDS));

        assertThat(thrown).isInstanceOf(ExecutionException.class)
                          .hasRootCauseInstanceOf(PulsarClientException.InvalidMessageException.class);
        assertThat(isRestartableException(thrown.getCause())).as("permanent send failure must fail the job").isFalse();
        verify(messageBuilder).sendAsync();
    }

    private Future<?> process(TestInbox inbox) {
        return executor.submit(() -> processor.process(0, inbox));
    }

    private void assertSentExactly(List<String> expected) {
        ArgumentCaptor<String> values = ArgumentCaptor.forClass(String.class);
        verify(messageBuilder, times(expected.size())).value(values.capture());
        verify(messageBuilder, times(expected.size())).sendAsync();
        assertThat(values.getAllValues()).containsExactlyElementsOf(expected);
    }

    private static List<String> input(int size) {
        return IntStream.range(0, size).mapToObj(Integer::toString).toList();
    }

    private static class TestClientSupplier implements SupplierEx<PulsarClient> {
        @Serial
        private static final long serialVersionUID = 1L;

        private final transient PulsarClient client;

        TestClientSupplier(PulsarClient client) {
            this.client = client;
        }

        @Override
        public PulsarClient getEx() {
            return client;
        }
    }
}
