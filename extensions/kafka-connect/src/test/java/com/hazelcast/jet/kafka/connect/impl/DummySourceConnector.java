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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class DummySourceConnector extends SourceConnector {

    static final String ITEMS_SIZE = "dummy.source.connector.items.size";
    static DummySourceConnector INSTANCE;
    private boolean started;
    private Map<String, String> properties;
    private boolean initialized;

    public DummySourceConnector() {
        INSTANCE = this;
    }

    @Override
    public void initialize(ConnectorContext ctx) {
        super.initialize(ctx);
        this.initialized = true;
    }

    @Override
    public void start(Map<String, String> properties) {
        this.properties = properties;
        started = true;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DummyTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            HashMap<String, String> taskConfig = new HashMap<>(properties);
            taskConfig.put("task.id", String.valueOf(i));
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
        started = false;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public String version() {
        return "dummy";
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void setProperty(String key, String value) {
        properties.put(key, value);
    }

    public void triggerReconfiguration() {
        context.requestTaskReconfiguration();
    }

    public static final class DummyTask extends SourceTask {
        static DummyTask INSTANCE;

        private static final HashMap<String, Object> SOURCE_PARTITION;

        private Map<String, String> properties;
        private boolean started;

        private final AtomicInteger counter = new AtomicInteger();

        private boolean initialized;
        private boolean wasCommit;

        static {
            SOURCE_PARTITION = new HashMap<>();
            SOURCE_PARTITION.put("sourcePartition", 1);
        }

        private List<SourceRecord> committedRecords = new ArrayList<>();

        public DummyTask() {
            super();
            INSTANCE = this;

        }

        @Override
        public void initialize(SourceTaskContext context) {
            super.initialize(context);
            Map<String, Object> offset = context.offsetStorageReader().offset(SOURCE_PARTITION);
            if (offset != null) {
                counter.set((Integer) offset.get("sourceOffset"));
            }
            this.initialized = true;
        }

        @Override
        public String version() {
            return "dummy";
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public void start(Map<String, String> properties) {
            this.properties = properties;
            this.started = true;
        }

        @Override
        public List<SourceRecord> poll() {
            int size = Integer.parseInt(properties.getOrDefault(ITEMS_SIZE, "0"));
            List<SourceRecord> sourceRecords = new ArrayList<>();

            for (int i = 0; i < size; i++) {
                sourceRecords.add(dummyRecord(counter.getAndIncrement()));
            }
            return sourceRecords;
        }

        @Nonnull
        static SourceRecord dummyRecord(int value) {
            Map<String, Object> sourceOffset = offset(value);
            return new SourceRecord(SOURCE_PARTITION, sourceOffset, "topic", 1, Schema.INT32_SCHEMA, value);
        }

        @Nonnull
        private static Map<String, Object> offset(int value) {
            HashMap<String, Object> sourceOffset = new HashMap<>();
            sourceOffset.put("sourceOffset", value);
            return sourceOffset;
        }

        @Override
        public void stop() {
            this.started = false;
        }

        public boolean isStarted() {
            return started;
        }

        public boolean isStopped() {
            return !started;
        }

        public boolean isInitialized() {
            return initialized;
        }

        @Override
        public void commit() {
            wasCommit = true;
        }

        @Override
        public void commitRecord(SourceRecord rec, RecordMetadata metadata) {
            committedRecords.add(rec);
        }

        public boolean recordCommitted(SourceRecord rec) {
            return committedRecords.contains(rec);
        }

        public boolean wasCommit() {
            return wasCommit;
        }
    }
}

