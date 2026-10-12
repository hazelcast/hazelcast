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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore;
import com.hazelcast.cp.internal.datastructures.snapshot.KeyValueDataChunk;
import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CPMapRegistrySnapshot {

    private Map<String, CPMapStore> mapStores = new HashMap<>();
    private Set<String> destroyed = new HashSet<>();

    public CPMapRegistrySnapshot() {
    }

    // FIXME: This still references to original CPMapStore,
    //  this is not in a point in time snapshotting.
    public CPMapRegistrySnapshot(CPMapRegistry cpMapRegistry) {
        mapStores.putAll(cpMapRegistry.getMapStores());
        destroyed.addAll(cpMapRegistry.getDestroyed());
    }

    public boolean isEmpty() {
        return mapStores.isEmpty() && destroyed.isEmpty();
    }

    public Map<String, CPMapStore> getMapStores() {
        return mapStores;
    }

    public Set<String> getDestroyed() {
        return destroyed;
    }

    /**
     * Converts the snapshot data into chunks for serialization, respecting
     * the configured maximum chunk size.
     */
    @Nonnull
    public List<KeyValueDataChunk> toChunks(long maxChunkSizeInBytes) {
        List<KeyValueDataChunk> chunks = new ArrayList<>();

        for (Map.Entry<String, CPMapStore> storeEntry : mapStores.entrySet()) {
            appendStoreChunks(chunks, storeEntry.getKey(), storeEntry.getValue(), maxChunkSizeInBytes);
        }

        for (String destroyedName : destroyed) {
            chunks.add(new KeyValueDataChunk(destroyedName, true));
        }

        return chunks;
    }

    private void appendStoreChunks(List<KeyValueDataChunk> chunks, String mapName, CPMapStore mapStore,
                                   long maxChunkSizeInBytes) {
        KeyValueDataChunk currentChunk = new KeyValueDataChunk(mapName);
        chunks.add(currentChunk);

        final boolean purgeEnabled = mapStore.isPurgeEnabled();
        final int mapSize = mapStore.size();
        final Map<Data, Long> entryTimestamps = purgeEnabled ? mapStore.getEntryTimestamps() : null;
        final Iterator<Map.Entry<Data, Data>> iterator = mapStore.iterator();

        while (iterator.hasNext()) {
            if (currentChunk.getChunkSizeInBytes() >= maxChunkSizeInBytes) {
                currentChunk = new KeyValueDataChunk(mapName);
                chunks.add(currentChunk);
            }

            Map.Entry<Data, Data> entry = iterator.next();
            appendEntry(currentChunk, entry, entryTimestamps, purgeEnabled, maxChunkSizeInBytes, mapSize);
        }
    }

    private void appendEntry(KeyValueDataChunk chunk, Map.Entry<Data, Data> entry, Map<Data, Long> entryTimestamps,
                             boolean purgeEnabled, long maxChunkSizeInBytes, int mapSize) {
        Data key = entry.getKey();
        Data value = entry.getValue();

        if (purgeEnabled) {
            long timestamp = entryTimestamps.get(key);
            chunk.add(key, value, timestamp, maxChunkSizeInBytes, mapSize);
        } else {
            chunk.add(key, value, maxChunkSizeInBytes, mapSize);
        }
    }

    @Override
    public String toString() {
        return "CPMapRegistrySnapshot{"
                + "mapStores=" + mapStores
                + ", destroyed=" + destroyed + '}';
    }
}
