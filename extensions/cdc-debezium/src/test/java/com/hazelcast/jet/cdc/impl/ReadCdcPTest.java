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
package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.impl.ReadCdcP.EventBatch;
import com.hazelcast.jet.cdc.impl.ReadCdcP.JetOffsetStorage;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.config.EdgeConfig.DEFAULT_QUEUE_SIZE;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static io.debezium.storage.kafka.offset.KafkaOffsetStoreConverter.jsonConverter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
@SuppressWarnings("unchecked")
public class ReadCdcPTest {
    private static final String CONNECTOR_NAME = "test";
    private static final Map<String, Object> PARTITION = Map.of("server", "postgres");
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.common.Heartbeat").field("ts_ms", Schema.INT64_SCHEMA).build();

    private final Converter converter = jsonConverter();
    private final Properties properties = new Properties();
    private final RecordCommitter<RecordChangeEvent<SourceRecord>> committer = mock(RecordCommitter.class);
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = mock(DebeziumEngine.class);
    private ReadCdcP<SourceRecord> processor;
    private TestOutbox outbox;
    private JetOffsetStorage store;

    @After
    public void tearDown() {
        if (processor != null) {
            processor.close();
        }
        if (store != null) {
            store.stop();
        }
    }

    @Test
    public void acknowledgesOnlyAfterSuccessfulSnapshot() throws Exception {
        init(AT_LEAST_ONCE, 10);
        var first = event(1);
        var second = event(2);
        enqueue(first, second);
        processor.complete();
        assertThat(outbox.queue(0)).containsExactly(first.record(), second.record());
        verifyNoInteractions(committer);

        State snapshot = saveSnapshot();
        assertThat(snapshot.getOffset(partitionKey())).isEqualTo(offset(2));
        verifyNoInteractions(committer);

        assertTrue(processor.snapshotCommitFinish(true));
        verify(committer).markProcessed(first);
        verify(committer).markProcessed(second);
        verify(committer).markBatchFinished();
        verifyNoMoreInteractions(committer);
    }

    @Test
    public void failedSnapshotRetainsRecordsForNextSuccessfulSnapshot() throws Exception {
        init(AT_LEAST_ONCE, 100, Long.MAX_VALUE);
        var first = event(1);
        enqueue(first);
        processor.complete();
        saveSnapshot();
        assertTrue(processor.snapshotCommitFinish(false));
        verifyNoInteractions(committer);

        var second = event(2);
        enqueue(second);
        processor.complete();
        assertThat(saveSnapshot().getOffset(partitionKey())).isEqualTo(offset(2));
        assertFalse(processor.snapshotCommitFinish(true));
        assertTrue(processor.snapshotCommitFinish(true));
        verify(committer).markProcessed(first);
        verify(committer).markProcessed(second);
        verify(committer, times(2)).markBatchFinished();
    }

    @Test
    public void closeDoesNotAcknowledgeRecordsAfterLastSnapshot() throws Exception {
        init(AT_LEAST_ONCE, 10);
        var first = event(1);
        enqueue(first);
        processor.complete();
        saveSnapshot();
        assertTrue(processor.snapshotCommitFinish(true));

        enqueue(event(2));
        processor.complete();
        processor.close();
        processor = null;
        verify(engine).close();
        verify(committer).markProcessed(first);
        verify(committer).markBatchFinished();
        verifyNoMoreInteractions(committer);
    }

    @Test
    public void closeDoesNotAcknowledgeFailedSnapshot() throws Exception {
        init(AT_LEAST_ONCE, 10);
        enqueue(event(1));
        processor.complete();
        saveSnapshot();
        processor.snapshotCommitFinish(false);
        processor.close();
        processor = null;
        verifyNoInteractions(committer);
    }

    @Test
    public void withoutSnapshotsAcknowledgesEmittedBatch() throws Exception {
        init(NONE, 10);
        var event = event(1);
        enqueue(event);
        processor.complete();
        verify(committer).markProcessed(event);
        verify(committer).markBatchFinished();
    }

    @Test
    public void queueCapacityAccountsForDefaultBatchSize() throws Exception {
        init(AT_LEAST_ONCE, 10, 1000);

        assertThat(properties.getProperty("max.batch.size")).isEqualTo("2048");
        assertThat(processor.itemsBuffer.remainingCapacity()).isEqualTo(1);
    }

    @Test
    public void queueCapacityAccountsForCustomBatchSizeAndRoundsDown() throws Exception {
        properties.setProperty("max.batch.size", "300");
        init(AT_LEAST_ONCE, 10, 1000);

        assertThat(properties.getProperty("max.batch.size")).isEqualTo("300");
        assertThat(processor.itemsBuffer.remainingCapacity()).isEqualTo(3);
    }

    @Test
    public void queueCapacityAllowsOneBatchLargerThanRecordLimit() throws Exception {
        properties.setProperty("max.batch.size", "2000");
        init(AT_LEAST_ONCE, 10, 1000);

        assertThat(processor.itemsBuffer.remainingCapacity()).isEqualTo(1);
    }

    @Test
    public void queueCapacityIsCappedForUnlimitedRecords() throws Exception {
        init(AT_LEAST_ONCE, 10, Long.MAX_VALUE);

        assertThat(processor.itemsBuffer.remainingCapacity()).isEqualTo(DEFAULT_QUEUE_SIZE);
    }

    @Test
    public void zeroBatchSizeIsRejected() {
        properties.setProperty("max.batch.size", "0");

        assertThatThrownBy(() -> init(AT_LEAST_ONCE, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max.batch.size");
    }

    @Test
    public void negativeBatchSizeIsRejected() {
        properties.setProperty("max.batch.size", "-1");

        assertThatThrownBy(() -> init(AT_LEAST_ONCE, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max.batch.size");
    }

    @Test(timeout = 10_000)
    public void fullSizedBatchesApplyRecordLimitUntilSuccessfulSnapshot() throws Exception {
        properties.setProperty("max.batch.size", "2");
        init(AT_LEAST_ONCE, 10, 5);
        var first = event(1);
        var second = event(2);
        var third = event(3);
        var fourth = event(4);
        var fifth = event(5);
        var sixth = event(6);
        enqueue(first, second);
        processor.complete();
        enqueue(third, fourth);
        processor.complete();
        enqueue(fifth, sixth);
        var bufferedBatch = processor.itemsBuffer.peek();

        assertFalse(processor.complete());
        assertThat(processor.itemsBuffer).containsExactly(bufferedBatch);
        assertThat(outbox.queue(0)).containsExactly(first.record(), second.record(), third.record(), fourth.record());
        verifyNoInteractions(committer);

        assertThat(saveSnapshot().getOffset(partitionKey())).isEqualTo(offset(4));
        assertTrue(processor.snapshotCommitFinish(false));
        assertFalse(processor.complete());
        assertThat(processor.itemsBuffer).containsExactly(bufferedBatch);
        verifyNoInteractions(committer);

        saveSnapshot();
        assertFalse(processor.snapshotCommitFinish(true));
        assertTrue(processor.snapshotCommitFinish(true));
        verify(committer).markProcessed(first);
        verify(committer).markProcessed(second);
        verify(committer).markProcessed(third);
        verify(committer).markProcessed(fourth);
        verify(committer, times(2)).markBatchFinished();
        verifyNoMoreInteractions(committer);

        assertFalse(processor.complete());
        assertThat(processor.itemsBuffer).isEmpty();
        assertThat(outbox.queue(0)).containsExactly(
                first.record(), second.record(), third.record(), fourth.record(), fifth.record(), sixth.record());
        verifyNoMoreInteractions(committer);
        assertThat(saveSnapshot().getOffset(partitionKey())).isEqualTo(offset(6));
        assertTrue(processor.snapshotCommitFinish(true));
        verify(committer).markProcessed(fifth);
        verify(committer).markProcessed(sixth);
        verify(committer, times(3)).markBatchFinished();
        verifyNoMoreInteractions(committer);
    }

    @Test(timeout = 10_000)
    public void fullPendingBatchesAppliesBackpressureUntilSuccessfulSnapshot() throws Exception {
        init(AT_LEAST_ONCE, 10, 1);
        var first = event(1);
        var second = event(2);
        enqueue(first);
        assertFalse(processor.complete());
        enqueue(second);
        var bufferedBatch = processor.itemsBuffer.peek();
        assertFalse(processor.itemsBuffer.offer(new EventBatch(List.of(event(3)), committer)));

        assertFalse(processor.complete());
        assertThat(processor.itemsBuffer).containsExactly(bufferedBatch);
        assertThat(outbox.queue(0)).containsExactly(first.record());
        verifyNoInteractions(committer);

        assertThat(saveSnapshot().getOffset(partitionKey())).isEqualTo(offset(1));
        assertTrue(processor.snapshotCommitFinish(false));
        assertFalse(processor.complete());
        assertThat(processor.itemsBuffer).containsExactly(bufferedBatch);
        assertThat(outbox.queue(0)).containsExactly(first.record());
        verifyNoInteractions(committer);

        assertThat(saveSnapshot().getOffset(partitionKey())).isEqualTo(offset(1));
        assertTrue(processor.snapshotCommitFinish(true));
        verify(committer).markProcessed(first);
        verify(committer).markBatchFinished();
        verifyNoMoreInteractions(committer);

        assertFalse(processor.complete());
        assertThat(processor.itemsBuffer).isEmpty();
        assertThat(outbox.queue(0)).containsExactly(first.record(), second.record());
        assertThat(saveSnapshot().getOffset(partitionKey())).isEqualTo(offset(2));
        assertTrue(processor.snapshotCommitFinish(true));
        verify(committer).markProcessed(second);
        verify(committer, times(2)).markBatchFinished();
        verifyNoMoreInteractions(committer);
    }

    @Test
    public void fullOutboxRetainsBatchAndStopsConsumingBufferedBatches() throws Exception {
        init(NONE, 1, 1);
        var first = event(1);
        var second = event(2);
        var third = event(3);
        enqueue(first, second);
        assertFalse(processor.complete());
        enqueue(third);
        var bufferedBatch = processor.itemsBuffer.peek();

        assertFalse(processor.complete());
        assertThat(processor.itemsBuffer).containsExactly(bufferedBatch);
        verifyNoInteractions(committer);

        assertThat(outbox.queue(0).poll()).isEqualTo(first.record());
        outbox.reset();
        assertFalse(processor.complete());
        assertThat(outbox.queue(0).poll()).isEqualTo(second.record());
        verify(committer).markProcessed(first);
        verify(committer).markProcessed(second);
        verify(committer).markBatchFinished();
        verifyNoMoreInteractions(committer);

        outbox.reset();
        assertFalse(processor.complete());
        assertThat(outbox.queue(0)).containsExactly(third.record());
        assertThat(processor.itemsBuffer).isEmpty();
        verify(committer).markProcessed(third);
        verify(committer, times(2)).markBatchFinished();
        verifyNoMoreInteractions(committer);
    }

    @Test
    public void snapshotWaitsForEntireBatchToBeEmitted() throws Exception {
        init(AT_LEAST_ONCE, 1);
        var first = event(1);
        var second = event(2);
        enqueue(first, second);
        processor.complete();
        assertFalse(processor.saveToSnapshot());
        assertThat(outbox.snapshotQueue()).isEmpty();
        verifyNoInteractions(committer);

        assertThat(outbox.queue(0).poll()).isEqualTo(first.record());
        outbox.reset();
        assertThat(saveSnapshot().getOffset(partitionKey())).isEqualTo(offset(2));
        assertThat(outbox.queue(0)).containsExactly(second.record());
        verifyNoInteractions(committer);
        assertTrue(processor.snapshotCommitFinish(true));
    }

    @Test
    public void batchArrivingDuringSnapshotIsNotAcknowledgedWithThatSnapshot() throws Exception {
        init(AT_LEAST_ONCE, 10);
        var first = event(1);
        enqueue(first);
        processor.complete();
        saveSnapshot();
        enqueue(event(2));
        processor.complete();
        assertThat(outbox.queue(0)).containsExactly(first.record());
        assertTrue(processor.snapshotCommitFinish(true));
        verify(committer).markProcessed(first);
        verify(committer).markBatchFinished();
        verifyNoMoreInteractions(committer);
    }

    @Test
    public void backingStoreExposesOnlyCommittedOffsetsAndDoesNotOverwriteProcessedOffsets() throws Exception {
        init(AT_LEAST_ONCE, 10);
        State restored = new State();
        restored.setOffset(partitionKey(), offset(1));
        processor.restoreFromSnapshot(broadcastKey("cdc-snapshot"), restored);

        WorkerConfig config = mock(WorkerConfig.class);
        when(config.getString("offset.storage.topic")).thenReturn("1:1:testVertex");
        store = new JetOffsetStorage();
        store.configure(config);
        store.start();

        enqueue(event(3));
        processor.complete();
        assertThat(store.get(List.of(partitionKey())).get()).containsEntry(partitionKey(), offset(1));

        // Use Kafka's writer to also verify that snapshot offsets use the same serialization format.
        OffsetStorageWriter writer = new OffsetStorageWriter(store, CONNECTOR_NAME, converter, converter);
        writer.offset(PARTITION, Map.of("lsn", 2));
        assertTrue(writer.beginFlush());
        writer.doFlush((error, result) -> { }).get();
        assertThat(store.get(List.of(partitionKey())).get()).containsEntry(partitionKey(), offset(2));
        assertThat(store.connectorPartitions(CONNECTOR_NAME)).contains(PARTITION);
        assertThat(saveSnapshot().getOffset(partitionKey())).isEqualTo(offset(3));
    }

    private void init(ProcessingGuarantee guarantee, int capacity) throws Exception {
        init(guarantee, capacity, 4096);
    }

    private void init(ProcessingGuarantee guarantee, int capacity, long maxAccumulatedRecords) throws Exception {
        properties.setProperty("name", CONNECTOR_NAME);
        properties.setProperty("connector.class", PostgresConnector.class.getName());
        processor = new ReadCdcP<>(properties, noEventTime(), record -> record, false);
        outbox = new TestOutbox(new int[]{capacity}, 1);
        TestProcessorContext context = new TestProcessorContext().setProcessingGuarantee(guarantee);
        context.setMaxProcessorAccumulatedRecords(maxAccumulatedRecords);
        processor.init(outbox, context);
        processor.engine = engine;
    }

    @SafeVarargs
    private void enqueue(RecordChangeEvent<SourceRecord>... records) {
        processor.itemsBuffer.add(new EventBatch(List.of(records), committer));
    }

    private State saveSnapshot() {
        outbox.reset();
        assertTrue(processor.saveToSnapshot());
        return (State) outbox.snapshotQueue().remove().getValue();
    }

    private RecordChangeEvent<SourceRecord> event(int lsn) {
        SourceRecord record = new SourceRecord(PARTITION, Map.of("lsn", lsn), "topic", null, null,
                VALUE_SCHEMA, new Struct(VALUE_SCHEMA).put("ts_ms", 0L));
        return () -> record;
    }

    private ByteBuffer partitionKey() {
        return ByteBuffer.wrap(converter.fromConnectData(CONNECTOR_NAME, null, List.of(CONNECTOR_NAME, PARTITION)));
    }

    private ByteBuffer offset(int lsn) {
        return ByteBuffer.wrap(converter.fromConnectData(CONNECTOR_NAME, null, Map.of("lsn", lsn)));
    }
}
