/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.raft.impl.dataservice;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RaftDataService implements SnapshotAwareService<Map<Long, Object>> {

    public static final String SERVICE_NAME = "RaftTestService";

    private final Map<Long, Object> values = new ConcurrentHashMap<>();

    public RaftDataService() {
    }

    public Object apply(long commitIndex, Object value) {
        assert !values.containsKey(commitIndex)
                : "Cannot apply " + value + "since commitIndex: " + commitIndex + " already contains: " + values.get(commitIndex);

        values.put(commitIndex, value);
        return value;
    }

    public Object get(long commitIndex) {
        return values.get(commitIndex);
    }

    public int size() {
        return values.size();
    }

    public Set<Object> values() {
        return new HashSet<>(values.values());
    }

    public Object[] valuesArray() {
        return values.entrySet().stream()
                .sorted(Comparator.comparingLong(Entry::getKey))
                .map(Entry::getValue)
                .toArray();
    }

    @Override
    public Map<Long, Object> takeSnapshot(CPGroupId groupId, long commitIndex) {
        Map<Long, Object> snapshot = new HashMap<>();
        for (Entry<Long, Object> e : values.entrySet()) {
            assert e.getKey() <= commitIndex : "Key: " + e.getKey() + ", commit-index: " + commitIndex;
            snapshot.put(e.getKey(), e.getValue());
        }

        return snapshot;
    }

    @Override
    public void restoreSnapshot(CPGroupId groupId, long commitIndex, Map<Long, Object> snapshot) {
        values.clear();
        values.putAll(snapshot);
    }
}
