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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.cdc.RecordMappingFunction;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.embedded.async.ConvertingAsyncEngineBuilderFactory;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.cdc.impl.Utils.markBatchFinished;
import static com.hazelcast.jet.cdc.impl.Utils.markProcessed;
import static com.hazelcast.jet.config.EdgeConfig.DEFAULT_QUEUE_SIZE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.EventTimeMapper.NO_NATIVE_TIME;
import static io.debezium.embedded.EmbeddedEngineConfig.OFFSET_STORAGE;
import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY;
import static io.debezium.storage.kafka.offset.KafkaOffsetStoreConverter.jsonConverter;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.connect.storage.OffsetUtils.processPartitionKey;

public class ReadCdcP<T> extends AbstractProcessor {

    public static final String NAME_PROPERTY = "name";
    public static final String CONNECTOR_CLASS_PROPERTY = "connector.class";
    public static final String SEQUENCE_EXTRACTOR_CLASS_PROPERTY = "sequence.extractor.class";

    private static final String TIMESTAMP_MS_FIELD_NAME = "ts_ms";

    private static final String JET_JOB_ID_REPLACEMENT_PROP_1 = "offset.storage.topic";
    private static final String JET_JOB_ID_REPLACEMENT_PROP_2 = "schema.history.internal.connector.id";
    private static final String MAX_BATCH_SIZE_NAME = "max.batch.size";
    private static final String DEFAULT_MAX_BATCH_SIZE = "2048";

    DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    ArrayBlockingQueue<EventBatch> itemsBuffer;
    private final Properties properties;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private final boolean trackSnapshots;
    private final RecordMappingFunction<T> recordMappingFunction;

    private Traverser<Object> traverser = Traversers.empty();
    private Traverser<Map.Entry<BroadcastKey<String>, State>> snapshotTraverser;
    private boolean snapshotInProgress;
    private boolean snapshottingEnabled;
    private ILogger logger;
    private int maxShutdownWaitSeconds;

    private ArrayBlockingQueue<EventBatch> pendingBatches;
    private final Converter offsetConverter = jsonConverter();
    private final AtomicBoolean engineStarted = new AtomicBoolean();
    private ExecutorService executor;

    private State state;
    private volatile CompletionEvent completionEvent;
    private Long snapshotTrackingId;
    private String vertexName;

    static {
        // workaround for https://github.com/hazelcast/hazelcast-jet/issues/2603
        DriverManager.getDrivers();
    }

    record EventBatch(
            List<RecordChangeEvent<SourceRecord>> records,
            RecordCommitter<RecordChangeEvent<SourceRecord>> committer) {
    }

    record CompletionEvent(boolean success, String message, Throwable throwable) {
    }

    public ReadCdcP(
        @Nonnull Properties properties,
        @Nonnull EventTimePolicy<? super T> eventTimePolicy,
        @Nonnull RecordMappingFunction<T> recordMappingFunction,
        boolean trackSnapshots) {
        this.properties = requireNonNull(properties, "properties cannot be null");
        this.recordMappingFunction = requireNonNull(recordMappingFunction, "recordMappingFunction cannot be null");

        this.eventTimeMapper = new EventTimeMapper<>(requireNonNull(eventTimePolicy, "eventTimePolicy cannot be null"));
        this.trackSnapshots = trackSnapshots;
        this.eventTimeMapper.addPartitions(1);
    }

    @Override
    protected void init(@Nonnull Context context) {
        if (!properties.containsKey(MAX_BATCH_SIZE_NAME)) {
            properties.setProperty(MAX_BATCH_SIZE_NAME, DEFAULT_MAX_BATCH_SIZE);
        }
        int maxCapacity = readMaxCapacity(context);
        this.pendingBatches = new ArrayBlockingQueue<>(maxCapacity);
        this.itemsBuffer = new ArrayBlockingQueue<>(maxCapacity);

        this.logger = context.logger();
        this.snapshottingEnabled = context.snapshottingEnabled();
        this.vertexName = context.vertexName();

        properties.setProperty(OFFSET_STORAGE.name(), JetOffsetStorage.class.getName());
        properties.setProperty(SCHEMA_HISTORY.name(), StatefulSchemaHistory.class.getName());

        String sourceId = context.jobId() + ":" + context.executionId() + ":" + context.vertexName();
        state = State.getOrCreate(sourceId);
        // due to Debezium providing only a subset of properties to SchemaHistory and OffsetBackingStore,
        // we need to reuse existing properties to provide the source state ID
        properties.setProperty(JET_JOB_ID_REPLACEMENT_PROP_1, sourceId);
        properties.setProperty(JET_JOB_ID_REPLACEMENT_PROP_2, sourceId);

        if (trackSnapshots) {
            final String channelsKey = "notification.enabled.channels";
            String channels = properties.containsKey(channelsKey)
                              ? properties.getProperty(channelsKey) + ","
                              : "";
            properties.setProperty(channelsKey, channels + "SnapshotCompletionListener");
            snapshotTrackingId = context.jobId();
            NotificationHoldingMap.INSTANCE.markNotYetDone(snapshotTrackingId, vertexName);
            properties.setProperty("notification.SnapshotCompletionListener.jobId", String.valueOf(snapshotTrackingId));
            properties.setProperty("notification.SnapshotCompletionListener.sourceVertexName", vertexName);
        }
        recordMappingFunction.init(properties, context.classLoader());

        this.executor = Executors.newFixedThreadPool(
                Integer.parseInt(properties.getProperty("tasks.max", "1")));

        if (logger.isFineEnabled()) {
            properties.forEach((k, v) -> {
                var key = (String) k;
                var value = v;
                if (key.contains("password")) {
                    value = "****";
                }
                logger.fine("Property '%s': %s", key, value);
            });
        }

        maxShutdownWaitSeconds = Integer.parseInt(properties.getProperty("max.shutdown.wait.s", "120"));

        this.engine = new ConvertingAsyncEngineBuilderFactory()
            .builder(ChangeEventFormat.of(Connect.class))
            .using(context.classLoader())
            .using(properties)
            .notifying((records, committer) -> itemsBuffer.put(new EventBatch(records, committer)))
            .using((success, message, error) -> completionEvent = new CompletionEvent(success, message, error))
            .build();
    }

    private int readMaxCapacity(Context context) {
        int maxBatchSize = checkPositive(MAX_BATCH_SIZE_NAME, Integer.parseInt(properties.getProperty(MAX_BATCH_SIZE_NAME)));
        // Both queues hold batches. Allow at least one batch so a limit below the batch size still makes progress.
        long maxBatches = Math.max(1, context.maxProcessorAccumulatedRecords() / maxBatchSize);
        return (int) Math.min(DEFAULT_QUEUE_SIZE, maxBatches);
    }

    private void runEngine() {
        if (engineStarted.compareAndSet(false, true)) {
            logger.fine("Engine was not running yet, starting");
            executor.execute(engine);
        }
    }

    @Override
    public boolean complete() {
        runEngine();
        checkCompleted();
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        if (snapshotInProgress) {
            return false;
        }
        if (snapshottingEnabled && pendingBatches.remainingCapacity() == 0) {
            return false;
        }

        var batch = itemsBuffer.poll();
        if (batch == null) {
            traverser = eventTimeMapper.flatMapIdle();
        } else {
            traverser = traverseIterable(batch.records())
                    .flatMap(event -> {
                        var sourceRecord = event.record();
                        if (sourceRecord == null) {
                            return eventTimeMapper.flatMapIdle();
                        }
                        long nativeTime = extractTimestamp(sourceRecord);
                        T mappedRecord = recordMappingFunction.apply(sourceRecord);
                        return eventTimeMapper.flatMapEvent(mappedRecord, 0, nativeTime);
                    })
                    .onFirstNull(() -> finishBatch(batch));
        }

        emitFromTraverser(traverser);
        return false;
    }

    private void finishBatch(EventBatch batch) {
        // Snapshot the offsets of emitted records independently of Debezium's periodic flushes.
        Map<Map<String, ?>, Map<String, ?>> offsets = new HashMap<>();
        for (var event : batch.records()) {
            SourceRecord record = event.record();
            if (record != null) {
                offsets.put(record.sourcePartition(), record.sourceOffset());
            }
        }
        String name = properties.getProperty(NAME_PROPERTY);
        offsets.forEach((partition, offset) -> state.setOffset(
                ByteBuffer.wrap(offsetConverter.fromConnectData(name, null, List.of(name, partition))),
                ByteBuffer.wrap(offsetConverter.fromConnectData(name, null, offset))));

        if (snapshottingEnabled) {
            // Even engine.close() flushes processed offsets. Only acknowledge records after
            // their Jet snapshot is durable, otherwise DB may discard WAL needed on restart.
            try {
                pendingBatches.put(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new JetException(e);
            }
        } else {
            acknowledgeBatch(batch);
        }
    }

    private static void acknowledgeBatch(EventBatch batch) {
        for (var event : batch.records()) {
            if (event.record() != null) {
                markProcessed(batch.committer(), event);
            }
        }
        markBatchFinished(batch.committer());
    }

    private void checkCompleted() {
        CompletionEvent event = this.completionEvent;
        if (event != null) {
            if (!event.success()) {
                throw new JetException(event.message(), event.throwable());
            }
        }
    }

    public static class StatefulSchemaHistory extends AbstractSchemaHistory {
        private static final ILogger LOGGER = Logger.getLogger(StatefulSchemaHistory.class);
        private State state;

        @Override
        public void configure(Configuration config, HistoryRecordComparator comparator,
                              SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
            super.configure(config, comparator, listener, useCatalogBeforeSchema);
            state = State.get(config.getString(JET_JOB_ID_REPLACEMENT_PROP_2));
            requireNonNull(state);
        }

        @Override
        protected void storeRecord(HistoryRecord historyRecord) throws SchemaHistoryException {
            LOGGER.fine("Storing history record: %s", historyRecord);
            state.addHistory(historyRecord);
        }

        @Override
        protected void recoverRecords(Consumer<HistoryRecord> records) {
            List<HistoryRecord> historyRecords = state.getHistoryRecords();
            LOGGER.fine("Recovering %d schema history records", historyRecords.size());
            historyRecords.forEach(records);
        }

        @Override
        public boolean exists() {
            List<HistoryRecord> historyRecords = state.getHistoryRecords();
            return historyRecords != null && !historyRecords.isEmpty();
        }

        @Override
        public boolean storageExists() {
            List<HistoryRecord> historyRecords = state.getHistoryRecords();
            return historyRecords != null;
        }
    }

    public static class JetOffsetStorage extends MemoryOffsetBackingStore {
        private final Map<String, Set<Map<String, Object>>> connectorPartitions = new HashMap<>();
        private Converter keyConverter;
        private State state;

        @Override
        public void configure(WorkerConfig config) {
            super.configure(config);
            state = State.get(config.getString(JET_JOB_ID_REPLACEMENT_PROP_1));
        }

        @Override
        public void start() {
            // Keep the offsets visible to Debezium separate from the processed offsets in State.
            // In particular, PostgreSQL reads this store when acknowledging WAL to the server.
            data.putAll(state.getPartitionsToOffset());
            keyConverter = jsonConverter();
            save();
            super.start();
        }

        @Override
        protected void save() {
            data.forEach(
                    (partition, offset) -> {
                        byte[] partitionBytes = partition.array();
                        byte[] offsetBytes = offset.array();
                        processPartitionKey(partitionBytes, offsetBytes, keyConverter, connectorPartitions);
                    });
        }

        @Override
        public Set<Map<String, Object>> connectorPartitions(String connectorName) {
            return connectorPartitions.getOrDefault(connectorName, emptySet());
        }

    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }
        snapshotInProgress = true;
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.singleton(entry(snapshotKey(), state))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        getLogger().finest("Finished saving snapshot.");
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    private BroadcastKey<String> snapshotKey() {
        return broadcastKey("cdc-snapshot");
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        if (success && !pendingBatches.isEmpty()) {
            acknowledgeBatch(pendingBatches.remove());
            // Give the execution service a chance to yield between batches.
            if (!pendingBatches.isEmpty()) {
                return false;
            }
        }
        snapshotInProgress = false;
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (!snapshotKey().equals(key)) {
            throw new RuntimeException("Unexpected key received from snapshot: " + key);
        }
        var logger = getLogger();
        if (logger.isFineEnabled()) {
            logger.fine("Restoring from snapshot: %s", value);
        }
        state.restore((State) value);
    }

    @Override
    public void close() {
        try {
            closeQuietly(engine);
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(maxShutdownWaitSeconds, SECONDS)) {
                        logger.warning("Cannot shutdown executor after " + maxShutdownWaitSeconds + " seconds");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new JetException(e);
                }
            }
        } finally {
            if (state != null) {
                state.remove();
            }
            if (trackSnapshots && snapshotTrackingId != null) {
                NotificationHoldingMap.INSTANCE.clearStatus(snapshotTrackingId, vertexName);
            }
        }
    }

    private static long extractTimestamp(SourceRecord sourceRecord) {
        Schema valueSchema = sourceRecord.valueSchema();
        boolean noValueTsMs = valueSchema.field(TIMESTAMP_MS_FIELD_NAME) == null;
        if (valueSchema.name().equalsIgnoreCase("io.debezium.connector.common.Heartbeat")) {
            return ((Struct) sourceRecord.value()).getInt64(TIMESTAMP_MS_FIELD_NAME);
        }

        Field field = valueSchema.field("source");
        boolean noSourceTsMs = field.schema().field(TIMESTAMP_MS_FIELD_NAME) == null;
        if (noValueTsMs && noSourceTsMs) {
            return NO_NATIVE_TIME;
        }
        Long timestamp;
        Struct valueStruct = (Struct) sourceRecord.value();
        if (noValueTsMs) {
            timestamp = valueStruct.getStruct("source").getInt64(TIMESTAMP_MS_FIELD_NAME);
        } else {
            timestamp = valueStruct.getInt64(TIMESTAMP_MS_FIELD_NAME);
        }
        return timestamp == null ? NO_NATIVE_TIME : timestamp;
    }

}
