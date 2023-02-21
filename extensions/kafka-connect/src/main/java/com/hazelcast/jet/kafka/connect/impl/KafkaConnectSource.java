/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.pipeline.SourceBuilder;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.client.impl.protocol.util.PropertiesUtil.toMap;
import static com.hazelcast.internal.util.Preconditions.checkRequiredProperty;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class KafkaConnectSource {

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
    private boolean taskInitialized;

    public KafkaConnectSource(Properties properties) {
        try {
            String connectorClazz = checkRequiredProperty(properties, "connector.class");
            Class<?> connectorClass = loadConnectorClass(connectorClazz);
            this.connector = (SourceConnector) connectorClass.getConstructor().newInstance();
            this.connector.initialize(new JetConnectorContext());
            this.connector.start(toMap(properties));

            this.taskConfig = connector.taskConfigs(1).get(0);
            this.task = (SourceTask) connector.taskClass().getConstructor().newInstance();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static Class<?> loadConnectorClass(String connectorClazz) {
        try {
            return Thread.currentThread().getContextClassLoader().loadClass(connectorClazz);
        } catch (ClassNotFoundException e) {
            throw new HazelcastException("Connector class '" + connectorClazz + "' not found. " +
                    "Did you add the connector jar to the job?", e);
        }
    }

    public void fillBuffer(SourceBuilder.TimestampedSourceBuffer<SourceRecord> buf) {
        if (!taskInitialized) {
            task.initialize(new JetSourceTaskContext());
            task.start(taskConfig);
            taskInitialized = true;
        }
        try {
            List<SourceRecord> records = task.poll();
            if (records == null) {
                return;
            }

            for (SourceRecord record : records) {
                addToBuffer(record, buf);
                partitionsToOffset.put(record.sourcePartition(), record.sourceOffset());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        }
    }

    private void addToBuffer(SourceRecord record, SourceBuilder.TimestampedSourceBuffer<SourceRecord> buf) {
        long ts = record.timestamp() == null ? 0 : record.timestamp();
        buf.add(record, ts);
    }

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

    private class JetSourceTaskContext implements SourceTaskContext {
        @Override
        public Map<String, String> configs() {
            return taskConfig;
        }

        @Override
        public OffsetStorageReader offsetStorageReader() {
            return new SourceOffsetStorageReader(partitionsToOffset);
        }
    }
}
