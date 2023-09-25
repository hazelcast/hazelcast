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

import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class SourceOffsetStorageReader implements OffsetStorageReader {

    private final State state;

    SourceOffsetStorageReader(State state) {
        this.state = state;
    }

    @Override
    public <V> Map<String, Object> offset(Map<String, V> partition) {
        return offsets(Collections.singletonList(partition)).get(partition);
    }

    @Override
    public <V> Map<Map<String, V>, Map<String, Object>> offsets(Collection<Map<String, V>> partitions) {
        Map<Map<String, V>, Map<String, Object>> map = new HashMap<>();
        for (Map<String, V> partition : partitions) {
            @SuppressWarnings("unchecked")
            Map<String, Object> offset = (Map<String, Object>) state.getOffset(partition);
            map.put(partition, offset);
        }
        return map;
    }
}
