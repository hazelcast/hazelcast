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
import org.apache.kafka.connect.util.Callback;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.cdc.impl.Utils.markBatchFinished;
import static com.hazelcast.jet.cdc.impl.Utils.markProcessed;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.core.EventTimeMapper.NO_NATIVE_TIME;
import static io.debezium.embedded.EmbeddedEngineConfig.OFFSET_STORAGE;
import static io.debezium.embedded.KafkaConnectUtil.converterForOffsetStore;
import static io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY;
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

    private final Properties properties;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private final RecordMappingFunction<T> recordMappingFunction;

    private Traverser<Object> traverser = Traversers.empty();
    private Traverser<Map.Entry<BroadcastKey<String>, State>> snapshotTraverser;
    private boolean snapshotInProgress;
    private ILogger logger;
    private int maxShutdownWaitSeconds;

    private final ConcurrentLinkedDeque<EventBatch> itemsBuffer = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean engineStarted = new AtomicBoolean();
    private DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private ExecutorService executor;

    private State state;
    private volatile CompletionEvent completionEvent;

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
            @Nonnull RecordMappingFunction<T> recordMappingFunction) {
        this.properties = requireNonNull(properties, "properties cannot be null");
        this.recordMappingFunction = requireNonNull(recordMappingFunction, "recordMappingFunction cannot be null");

        this.eventTimeMapper = new EventTimeMapper<>(requireNonNull(eventTimePolicy, "eventTimePolicy cannot be null"));
        this.eventTimeMapper.addPartitions(1);
    }

    @Override
    protected void init(@Nonnull Context context) {
        this.logger = context.logger();

        properties.setProperty(OFFSET_STORAGE.name(), JetOffsetStorage.class.getName());
        properties.setProperty(SCHEMA_HISTORY.name(), StatefulSchemaHistory.class.getName());

        state = State.getOrCreate(context.jobId());
        // due to Debezium providing only a subset of properties to SchemaHistory and OffsetBackingStore,
        // we need to reuse existing properties to provide job id
        properties.setProperty(JET_JOB_ID_REPLACEMENT_PROP_1, String.valueOf(context.jobId()));
        properties.setProperty(JET_JOB_ID_REPLACEMENT_PROP_2, String.valueOf(context.jobId()));

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
            .notifying((records, committer) -> itemsBuffer.add(new EventBatch(records, committer)))
            .using((success, message, error) -> completionEvent = new CompletionEvent(success, message, error))
            .build();
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

        var batch = itemsBuffer.poll();
        if (batch == null) {
            traverser = eventTimeMapper.flatMapIdle();
        } else {
            var committer = batch.committer();
            traverser = traverseIterable(batch.records())
                    .flatMap(event -> {
                        var sourceRecord = event.record();
                        if (sourceRecord == null) {
                            return eventTimeMapper.flatMapIdle();
                        }
                        long nativeTime = extractTimestamp(sourceRecord);
                        T mappedRecord = recordMappingFunction.apply(sourceRecord);
                        var mapped = eventTimeMapper.flatMapEvent(mappedRecord, 0, nativeTime);
                        markProcessed(committer, event);
                        return mapped;
                    })
                    .onFirstNull(() -> markBatchFinished(committer));
        }

        emitFromTraverser(traverser);
        return false;
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
            long jobId = Long.parseLong(config.getString(JET_JOB_ID_REPLACEMENT_PROP_2));
            state = State.get(jobId);
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
            state = State.get(Long.parseLong(config.getString(JET_JOB_ID_REPLACEMENT_PROP_1)));
        }

        @Override
        public void start() {
            super.start();
            keyConverter = converterForOffsetStore();
            state.getPartitionsToOffset().forEach((partition, offset) -> {
                byte[] partitionBytes = partition.array();
                byte[] offsetBytes = offset.array();
                processPartitionKey(partitionBytes, offsetBytes, keyConverter, connectorPartitions);
            });
        }

        @Override
        public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
            return executor.submit(() -> {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
                for (ByteBuffer key : keys) {
                    ByteBuffer offset = state.getOffset(key);
                    if (offset != null) {
                        result.put(key, offset);
                    }
                }
                return result;
            });
        }

        @Override
        public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
            return executor.submit(() -> {
                values.forEach((k, v) -> state.setOffset(k, v));

                if (callback != null) {
                    callback.onCompletion(null, null);
                }
                return null;
            });
        }

        @Override
        protected void save() {
            state.getPartitionsToOffset().forEach(
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
        closeQuietly(engine);
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(maxShutdownWaitSeconds, SECONDS)) {
                    logger.warning("Cannot shutdown executor after " + maxShutdownWaitSeconds + " seconds");
                }
            } catch (InterruptedException e) {
                throw new JetException(e);
            }
        }
        if (state != null) {
            state.remove();
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
