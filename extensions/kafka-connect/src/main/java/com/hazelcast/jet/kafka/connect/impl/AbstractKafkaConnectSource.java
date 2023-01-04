/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.jet.pipeline.SourceBuilder;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public abstract class AbstractKafkaConnectSource<T> {

    private final SourceConnector connector;
    private final SourceTask task;
    private final Map<String, String> taskConfig;


    /**
     * Key represents the partition which the record originated from. Value
     * represents the offset within that partition. Kafka Connect represents
     * the partition and offset as arbitrary values so that is why it is
     * stored as map.
     * See {@link SourceRecord} for more information regarding the format.
     */
    private Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new HashMap<>();
    private boolean taskInit;

    public AbstractKafkaConnectSource(Properties properties) {
        try {
            String connectorClazz = properties.getProperty("connector.class");
            Class<?> connectorClass = Thread.currentThread().getContextClassLoader().loadClass(connectorClazz);
            connector = (SourceConnector) connectorClass.getConstructor().newInstance();
            connector.initialize(new JetConnectorContext());
            connector.start((Map) properties);

            taskConfig = connector.taskConfigs(1).get(0);
            task = (SourceTask) connector.taskClass().getConstructor().newInstance();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    public void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> buf) {
        if (!taskInit) {
            task.initialize(new JetSourceTaskContext());
            task.start(taskConfig);
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
                    partitionsToOffset.put(record.sourcePartition(), record.sourceOffset());
                }
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    protected abstract boolean addToBuffer(SourceRecord record, SourceBuilder.TimestampedSourceBuffer<T> buf);

    public void destroy() {
        try {
            task.stop();
        } finally {
            connector.stop();
        }
    }

    public Map<Map<String, ?>, Map<String, ?>> createSnapshot() {
        return partitionsToOffset;
    }

    public void restoreSnapshot(List<Map<Map<String, ?>, Map<String, ?>>> snapshots) {
        this.partitionsToOffset = snapshots.get(0);
    }

    private static class JetConnectorContext implements ConnectorContext {
        @Override
        public void requestTaskReconfiguration() {
            // no-op since it is not supported
        }

        @Override
        public void raiseError(Exception e) {
            rethrow(e);
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
                Map<String, Object> offset = (Map<String, Object>) partitionsToOffset.get(partition);
                map.put(partition, offset);
            }
            return map;
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
}
