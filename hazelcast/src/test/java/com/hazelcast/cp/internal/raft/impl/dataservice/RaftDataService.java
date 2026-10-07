/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cp.internal.datastructures.snapshot.DataChunkGroup;
import com.hazelcast.cp.internal.raft.ChunkedSnapshotAwareService;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RaftDataService implements ChunkedSnapshotAwareService<Map<Long, Object>, Map<Long, Object>> {

    public static final String SERVICE_NAME = "RaftTestService";

    private static final int MAX_AMOUNT_IN_A_CHUNK = 10;

    private final Map<Long, Object> values = new ConcurrentHashMap<>();

    public RaftDataService() {
    }

    public Object apply(long commitIndex, Object value) {
        assert !values.containsKey(commitIndex)
                : "Cannot apply " + value + "since commitIndex: "
                + commitIndex + " already contains: " + values.get(commitIndex);

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
    public Iterator<DataChunkGroup<Map<Long, Object>>> takeSnapshotChunks(@Nonnull CPGroupId groupId,
                                                                          long commitIndex) {
        Map<Long, Object> snapshot = takeSnapshot(groupId, commitIndex);
        return createChunks(snapshot).iterator();
    }

    private static List<DataChunkGroup<Map<Long, Object>>> createChunks(Map<Long, Object> snap) {
        List<DataChunkGroup<Map<Long, Object>>> dataChunks = new ArrayList<>();

        List<Map<Long, Object>> maps = splitMap(snap, MAX_AMOUNT_IN_A_CHUNK);
        for (Map<Long, Object> map : maps) {
            dataChunks.add(new DataChunkGroup<Map<Long, Object>>().add(map));
        }
        return dataChunks;
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
    public void restoreSnapshotChunk(@Nonnull CPGroupId groupId, long commitIndex,
                                     @Nonnull DataChunkGroup<Map<Long, Object>> dataChunk) {
        List<Map<Long, Object>> data = dataChunk.getServiceData();
        for (Map<Long, Object> map : data) {
            values.putAll(map);
        }
    }

    @Override
    public void prepareForSnapshotRestore(@Nonnull CPGroupId groupId) {
        values.clear();
    }

    /**
     * Splits a given map into multiple smaller
     * maps based on the specified size.
     *
     * @param originalMap The original map to be split. Must
     *                    not be null.
     * @param splitSize   The maximum number of
     *                    entries each smaller map should contain. Must be greater
     *                    than 0.
     * @return A list of maps, where each map contains
     * up to {@code splitSize} entries from the original map.
     */
    private static List<Map<Long, Object>> splitMap(Map<Long, Object> originalMap, int splitSize) {
        assert originalMap != null;
        assert splitSize > 0;

        List<Map<Long, Object>> result = new ArrayList<>();
        Map<Long, Object> currentMap = new HashMap<>();

        int count = 0;
        for (Map.Entry<Long, Object> entry : originalMap.entrySet()) {
            currentMap.put(entry.getKey(), entry.getValue());
            count++;

            if (count == splitSize) {
                result.add(currentMap);
                currentMap = new HashMap<>();
                count = 0;
            }
        }

        // Add the remaining entries if any
        if (!currentMap.isEmpty()) {
            result.add(currentMap);
        }

        return result;
    }
}
