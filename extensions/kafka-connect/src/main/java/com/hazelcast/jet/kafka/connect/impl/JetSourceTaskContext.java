/*
 * Copyright 2024 Hazelcast Inc.
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

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Map;

/**
 * SourceTaskContext is provided to SourceTasks to allow them to interact with the underlying
 * runtime.
 */
class JetSourceTaskContext implements SourceTaskContext {
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
        return new JetSourceOffsetStorageReader(state);
    }
}
