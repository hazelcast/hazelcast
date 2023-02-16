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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.client.impl.protocol.util.PropertiesUtil.toMap;
import static com.hazelcast.internal.util.Preconditions.checkRequiredProperty;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class KafkaConnectSource {
    private static final ILogger LOGGER = Logger.getLogger(KafkaConnectSource.class);
    private static final int AWAIT_STOP_TIMEOUT = 1_000;

    private final SourceConnector connector;

    private CountDownLatch taskStoppedLatch = new CountDownLatch(1);

    private SourceTask task;


    /**
     * Key represents the partition which the record originated from. Value
     * represents the offset within that partition. Kafka Connect represents
     * the partition and offset as arbitrary values so that is why it is
     * stored as map.
     * See {@link SourceRecord} for more information regarding the format.
     */
    private Map<Map<String, ?>, Map<String, ?>> partitionsToOffset = new ConcurrentHashMap<>();
    private final AtomicBoolean taskRunning = new AtomicBoolean();

    private volatile boolean taskReconfigurationRequested = false;


    public KafkaConnectSource(Properties properties) {
        String connectorClazz = checkRequiredProperty(properties, "connector.class");
        Class<?> connectorClass = loadConnectorClass(connectorClazz);
        this.connector = (SourceConnector) newInstance(connectorClass);
        this.connector.initialize(new JetConnectorContext());
        this.connector.start(toMap(properties));
    }

    @Nonnull
    private <T> T newInstance(Class<? extends T> clazz) {
        try {
            return clazz.getConstructor().newInstance();
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
        restartTaskIfNeeded();
        try {
            List<SourceRecord> records = task.poll();
            if (records == null) {
                return;
            }
            if (LOGGER.isFineEnabled()) {
                LOGGER.fine("Get " + records.size() + " records from task poll");
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

    private void restartTaskIfNeeded() {
        if (taskReconfigurationRequested) {
            taskReconfigurationRequested = false;
            stopTaskAndAwait();
        }
        if (taskRunning.get()) {
            LOGGER.info("Not starting task, already running");
            return;
        }
        startTaskIfNotRunning();
    }

    private void startTaskIfNotRunning() {
        LOGGER.info("Starting tasks if the previous not running");
        if (taskRunning.compareAndSet(false, true)) {
            LOGGER.info("Starting tasks");

            List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
            if (!taskConfigs.isEmpty()) {
                Map<String, String> taskConfig = taskConfigs.get(0);
                this.task = newInstance(connector.taskClass().asSubclass(SourceTask.class));
                LOGGER.info("Initializing task: " + task);
                task.initialize(new JetSourceTaskContext(taskConfig));
                LOGGER.info("Starting task: " + task + " with task config: " + taskConfig);
                task.start(taskConfig);
            } else {
                LOGGER.info("No task configs!");
            }
        }
    }

    private void stopTaskAndAwait() {
        stopTaskIfRunning();
        if (!awaitStop()) {
            LOGGER.info("Waiting for stopping task timed-out");
        }
    }

    private boolean awaitStop() {
        try {
            LOGGER.info("Waiting for stopping the previous task");
            return taskStoppedLatch.await(AWAIT_STOP_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    private void addToBuffer(SourceRecord record, SourceBuilder.TimestampedSourceBuffer<SourceRecord> buf) {
        long ts = record.timestamp() == null ? 0 : record.timestamp();
        buf.add(record, ts);
    }

    public void destroy() {
        try {
            stopTaskIfRunning();
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

    private class JetConnectorContext implements ConnectorContext {

        @Override
        public void requestTaskReconfiguration() {
            LOGGER.info("Task reconfiguration requested");
            taskReconfigurationRequested = true;
        }

        @Override
        public void raiseError(Exception e) {
            throw rethrow(e);
        }
    }

    private void stopTaskIfRunning() {
        LOGGER.info("Stopping task if running");
        if (taskRunning.compareAndSet(true, false)) {
            try {
                if (task != null) {
                    LOGGER.fine("Stopping task");
                    task.stop();
                    LOGGER.fine("Task stopped");
                }
            } finally {
                taskStoppedLatch.countDown();
                taskStoppedLatch = new CountDownLatch(1);
                task = null;
            }
        }
    }

    private class JetSourceTaskContext implements SourceTaskContext {
        private final Map<String, String> taskConfig;

        private JetSourceTaskContext(Map<String, String> taskConfig) {
            this.taskConfig = taskConfig;
        }

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
