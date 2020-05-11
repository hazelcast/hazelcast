/*
 * Copyright 2020 Hazelcast Inc.
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

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.transforms.ExtractNewRecordState;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class CdcSource {

    private static final ThreadLocal<List<byte[]>> THREAD_LOCAL_HISTORY = new ThreadLocal<>();

    private final SourceConnector connector;
    private final SourceTask task;
    private final Map<String, String> taskConfig;
    private final ExtractNewRecordState<SourceRecord> transform;

    private State state = new State();
    private boolean taskInit;

    CdcSource(Properties properties) {
        try {
            String connectorClazz = properties.getProperty("connector.class");
            Class<?> connectorClass = Thread.currentThread().getContextClassLoader().loadClass(connectorClazz);
            connector = (SourceConnector) connectorClass.getConstructor().newInstance();
            connector.initialize(new JetConnectorContext());
            connector.start((Map) properties);

            transform = initTransform();

            taskConfig = connector.taskConfigs(1).get(0);
            task = (SourceTask) connector.taskClass().getConstructor().newInstance();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    public void fillBuffer(SourceBuilder.TimestampedSourceBuffer<ChangeRecord> buf) {
        if (!taskInit) {
            task.initialize(new JetSourceTaskContext());

            // Our DatabaseHistory implementation will be created by the
            // following start() call, on this thread (blocking worker
            // thread) and this is how we pass it the list it should
            // use for storing history records.
            THREAD_LOCAL_HISTORY.set(state.historyRecords);
            task.start(taskConfig);
            THREAD_LOCAL_HISTORY.remove();

            taskInit = true;
        }
        try {
            List<SourceRecord> records = task.poll();
            if (records == null) {
                return;
            }

            for (SourceRecord record : records) {
                boolean added = addToBuffer(record, buf);
                if (added) {
                    state.setOffset(record.sourcePartition(), record.sourceOffset());
                }
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private boolean addToBuffer(SourceRecord sourceRecord, SourceBuilder.TimestampedSourceBuffer<ChangeRecord> buf) {
        sourceRecord = transform.apply(sourceRecord);
        if (sourceRecord != null) {
            ChangeRecord changeRecord = toChangeRecord(sourceRecord);
            long timestamp = extractTimestamp(sourceRecord);
            buf.add(changeRecord, timestamp);
            return true;
        }
        return false;
    }

    public void destroy() {
        try {
            task.stop();
        } finally {
            connector.stop();
        }
    }

    public State createSnapshot() {
        return state;
    }

    public void restoreSnapshot(List<State> snapshots) {
        this.state = snapshots.get(0);
    }

    private static ExtractNewRecordState<SourceRecord> initTransform() {
        ExtractNewRecordState<SourceRecord> transform = new ExtractNewRecordState<>();

        Map<String, String> config = new HashMap<>();
        config.put("add.fields", "op, ts_ms");
        config.put("delete.handling.mode", "rewrite");
        transform.configure(config);

        return transform;
    }

    private static ChangeRecord toChangeRecord(SourceRecord record) {
        String keyJson = Values.convertToString(record.keySchema(), record.key());
        String valueJson = Values.convertToString(record.valueSchema(), record.value());
        return new ChangeRecordImpl(keyJson, valueJson);
    }

    private static long extractTimestamp(SourceRecord record) {
        if (record.valueSchema().field("__ts_ms") == null) {
            return 0L;
        } else {
            return ((Struct) record.value()).getInt64("__ts_ms");
        }
    }

    private class JetSourceTaskContext implements SourceTaskContext {
        @Override
        public Map<String, String> configs() {
            return taskConfig;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return new SourceOffsetStorageReader();
        }
    }

    private class SourceOffsetStorageReader implements OffsetStorageReader {
        @Override
        public <V> Map<String, Object> offset(Map<String, V> partition) {
            return offsets(Collections.singletonList(partition)).get(partition);
        }

        @Override
        public <V> Map<Map<String, V>, Map<String, Object>> offsets(Collection<Map<String, V>> partitions) {
            Map<Map<String, V>, Map<String, Object>> map = new HashMap<>();
            for (Map<String, V> partition : partitions) {
                Map<String, Object> offset = (Map<String, Object>) state.getOffset(partition);
                map.put(partition, offset);
            }
            return map;
        }
    }

    private static class JetConnectorContext implements ConnectorContext {
        @Override
        public void requestTaskReconfiguration() {
            // no-op since it is not supported
        }

        @Override
        public void raiseError(Exception e) {
            throw rethrow(e);
        }
    }

    public static final class State implements IdentifiedDataSerializable {

        /**
         * Key represents the partition which the record originated from. Value
         * represents the offset within that partition. Kafka Connect represents
         * the partition and offset as arbitrary values so that is why it is
         * stored as map.
         * See {@link SourceRecord} for more information regarding the format.
         */
        private final Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();

        /**
         * We use a copy-on-write-list because it will be written on a
         * different thread (some internal Debezium snapshot thread) than
         * is normally used to run the connector (one of Jet's blocking
         * worker threads).
         * <p>
         * The performance penalty of copying the list is also acceptable
         * since this list will be written rarely after the initial snapshot,
         * only on table schema changes.
         */
        private final List<byte[]> historyRecords = new CopyOnWriteArrayList<>();

        public Map<String, ?> getOffset(Map<String, ?> partition) {
            return partitionsToOffset.get(partition);
        }

        public void setOffset(Map<String, ?> partition, Map<String, ?> offset) {
            partitionsToOffset.put(partition, offset);
        }

        @Override
        public int getFactoryId() {
            return CdcJsonDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CdcJsonDataSerializerHook.SOURCE_STATE;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(partitionsToOffset);
            out.writeObject(historyRecords);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            partitionsToOffset.putAll(in.readObject());
            historyRecords.addAll(in.readObject());
        }
    }

    public static class DatabaseHistoryImpl extends AbstractDatabaseHistory {

        private final List<byte[]> history;

        public DatabaseHistoryImpl() {
            this.history = Objects.requireNonNull(THREAD_LOCAL_HISTORY.get());
        }

        @Override
        protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
            history.add(DocumentWriter.defaultWriter().writeAsBytes(record.document()));
        }

        @Override
        protected void recoverRecords(Consumer<HistoryRecord> consumer) {
            try {
                for (byte[] record : history) {
                    Document doc = DocumentReader.defaultReader().read(record);
                    consumer.accept(new HistoryRecord(doc));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean exists() {
            return history != null && !history.isEmpty();
        }
    }
}
