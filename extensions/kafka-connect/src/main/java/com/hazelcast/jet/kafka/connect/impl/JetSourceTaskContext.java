package com.hazelcast.jet.kafka.connect.impl;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Map;

final class JetSourceTaskContext implements SourceTaskContext {
    private final Map<String, String> taskConfig;
    private final State state;

    JetSourceTaskContext(Map<String, String> taskConfig,
                         State state) {
        this.taskConfig = taskConfig;
        this.state = state;
    }

    @Override
    public Map<String, String> configs() {
        return taskConfig;
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return new SourceOffsetStorageReader(state);
    }
}
