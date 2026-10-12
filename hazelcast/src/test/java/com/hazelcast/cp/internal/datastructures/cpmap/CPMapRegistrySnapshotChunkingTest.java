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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore;
import com.hazelcast.cp.internal.datastructures.cpmap.store.HeapCPMapStore;
import com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil;
import com.hazelcast.cp.internal.datastructures.snapshot.DataChunkGroup;
import com.hazelcast.cp.internal.datastructures.snapshot.KeyValueDataChunk;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMapRegistrySnapshotChunkingTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    private static final String GROUP_NAME = "test-group";
    private static final String MAP_NAME = "test-map";
    // Small size for testing
    private static final int CHUNK_SIZE_MB = 1;

    @Mock
    private CPGroupId groupId;

    @Mock
    private Data mockKey;

    @Mock
    private Data mockValue;

    private CPMapRegistrySnapshot cpMapRegistrySnapshot;
    private long maxChunkSizeInBytes;
    private CPMapRegistry cpMapRegistry;

    @Before
    public void setUp() {
        when(groupId.getName()).thenReturn(GROUP_NAME);

        cpMapRegistry = new CPMapRegistry(groupId);
        cpMapRegistrySnapshot = new CPMapRegistrySnapshot(cpMapRegistry);
        maxChunkSizeInBytes = MemoryUnit.MEGABYTES.toBytes(CHUNK_SIZE_MB);
    }

    @Test
    public void testToChunks_EmptyRegistry() {
        List<DataChunkGroup<KeyValueDataChunk>> dataChunks
                = ChunkUtil.groupServiceChunksBySize(cpMapRegistrySnapshot.toChunks(maxChunkSizeInBytes), maxChunkSizeInBytes);
        assertTrue("Empty registry should produce empty chunks list", dataChunks.isEmpty());
    }

    @Test
    public void testToChunks_SingleMapWithinChunkSize() {
        // Setup mock data sizes that will fit in one chunk
        when(mockKey.dataSize()).thenReturn(100);
        when(mockValue.dataSize()).thenReturn(100);

        // Create a mock CPMapStore with a single entry
        CPMapStore mockStore = createMockMapStore(1);
        cpMapRegistrySnapshot.getMapStores().put(MAP_NAME, mockStore);

        List<DataChunkGroup<KeyValueDataChunk>> dataChunks
                = ChunkUtil.groupServiceChunksBySize(cpMapRegistrySnapshot.toChunks(maxChunkSizeInBytes), maxChunkSizeInBytes);

        assertEquals("Should create single chunk", 1, dataChunks.size());
        assertNotNull("Chunk should be instance of CPMapSnapshotChunk", dataChunks.get(0));
    }

    @Test
    public void testToChunks_MultipleChunksRequired() {
        // Setup mock data sizes that will force multiple chunks
        when(mockKey.dataSize()).thenReturn(500_000);
        when(mockValue.dataSize()).thenReturn(500_000);

        // Create a mock CPMapStore with multiple entries
        CPMapStore mockStore = createMockMapStore(5);
        cpMapRegistrySnapshot.getMapStores().put(MAP_NAME, mockStore);

        List<DataChunkGroup<KeyValueDataChunk>> dataChunks
                = ChunkUtil.groupServiceChunksBySize(cpMapRegistrySnapshot.toChunks(maxChunkSizeInBytes), maxChunkSizeInBytes);

        assertTrue("Should create multiple chunks", dataChunks.size() > 1);
    }

    @Test
    public void testToChunks_WithDestroyedMaps() {
        // Add a destroyed map name
        cpMapRegistrySnapshot.getDestroyed().add("destroyed-map");

        List<DataChunkGroup<KeyValueDataChunk>> dataChunks
                = ChunkUtil.groupServiceChunksBySize(cpMapRegistrySnapshot.toChunks(maxChunkSizeInBytes), maxChunkSizeInBytes);

        assertEquals("Should create single chunk for destroyed map", 1, dataChunks.size());
        DataChunkGroup<KeyValueDataChunk> dataChunk = dataChunks.get(0);
        assertTrue("Chunk should contain destroyed map data",
                dataChunk.getServiceData().stream()
                        .anyMatch(data -> data.getName().equals("destroyed-map") && data.isDestroyed()));
    }

    @Test
    public void testResetForChunkedSnapshotRestore() {
        // Add some data to the registry
        cpMapRegistry.getMapStores().put(MAP_NAME, new HeapCPMapStore(100));
        cpMapRegistry.getDestroyed().add("destroyed-map");

        // Perform reset
        cpMapRegistry.resetForChunkedSnapshotRestore();

        assertTrue("MapStores should be empty after reset",
                cpMapRegistry.getMapStores().isEmpty());
        assertTrue("Destroyed should be empty after reset",
                cpMapRegistry.getDestroyed().isEmpty());
    }

    @Test
    public void testToChunks_MultipleMapsInSingleChunk() {
        // Setup mock data sizes that will fit multiple maps in one chunk
        when(mockKey.dataSize()).thenReturn(100);
        when(mockValue.dataSize()).thenReturn(100);

        // Add multiple maps with small data
        cpMapRegistrySnapshot.getMapStores().put("map1", createMockMapStore(1));
        cpMapRegistrySnapshot.getMapStores().put("map2", createMockMapStore(1));
        cpMapRegistrySnapshot.getMapStores().put("map3", createMockMapStore(1));

        List<DataChunkGroup<KeyValueDataChunk>> dataChunks
                = ChunkUtil.groupServiceChunksBySize(cpMapRegistrySnapshot.toChunks(maxChunkSizeInBytes), maxChunkSizeInBytes);

        assertEquals("Should create single chunk", 1, dataChunks.size());
        DataChunkGroup<KeyValueDataChunk> dataChunk = dataChunks.get(0);

        Set<String> mapNames = dataChunk.getServiceData().stream()
                .map(KeyValueDataChunk::getName)
                .collect(Collectors.toSet());

        assertTrue("Chunk should contain all maps",
                mapNames.containsAll(List.of("map1", "map2", "map3")));
    }

    @Test
    public void testToChunks_MultipleMapsAcrossChunks() {
        // Setup mock data sizes that will force chunks to split
        when(mockKey.dataSize()).thenReturn(400_000);
        when(mockValue.dataSize()).thenReturn(400_000);

        // Add multiple maps with large data
        cpMapRegistrySnapshot.getMapStores().put("map1", createMockMapStore(2));
        cpMapRegistrySnapshot.getMapStores().put("map2", createMockMapStore(2));
        cpMapRegistrySnapshot.getMapStores().put("map3", createMockMapStore(2));

        List<DataChunkGroup<KeyValueDataChunk>> dataChunks
                = ChunkUtil.groupServiceChunksBySize(cpMapRegistrySnapshot.toChunks(maxChunkSizeInBytes), maxChunkSizeInBytes);

        assertTrue("Should create multiple chunks", dataChunks.size() > 1);

        // Verify all maps are distributed across chunks
        Set<String> allMapNames = dataChunks.stream()
                .map(chunk -> chunk)
                .flatMap(chunk -> chunk.getServiceData().stream())
                .map(KeyValueDataChunk::getName)
                .collect(Collectors.toSet());

        assertEquals("All maps should be present in chunks",
                Set.of("map1", "map2", "map3"), allMapNames);
    }

    @Test
    public void testToChunks_MixedSizeMaps() {
        // Create maps with different sizes
        when(mockKey.dataSize()).thenReturn(100);
        when(mockValue.dataSize()).thenReturn(100);
        cpMapRegistrySnapshot.getMapStores().put("small_map", createMockMapStore(1));

        Data largeKey = mock(Data.class);
        Data largeValue = mock(Data.class);
        when(largeKey.dataSize()).thenReturn(500_000);
        when(largeValue.dataSize()).thenReturn(500_000);

        CPMapStore largeStore = mock(CPMapStore.class);
        List<AbstractMap.SimpleEntry<Data, Data>> simpleEntries = List.of(
                new AbstractMap.SimpleEntry<>(largeKey, largeValue),
                new AbstractMap.SimpleEntry<>(largeKey, largeValue),
                new AbstractMap.SimpleEntry<>(largeKey, largeValue));
        Iterator<AbstractMap.SimpleEntry<Data, Data>> iterator = simpleEntries.iterator();

        when(largeStore.iterator()).thenReturn((Iterator) iterator);
        when(largeStore.size()).thenReturn(simpleEntries.size());

        cpMapRegistrySnapshot.getMapStores().put("large_map", largeStore);

        List<DataChunkGroup<KeyValueDataChunk>> dataChunks
                = ChunkUtil.groupServiceChunksBySize(cpMapRegistrySnapshot.toChunks(maxChunkSizeInBytes), maxChunkSizeInBytes);

        assertEquals("Should create multiple chunks for mixed size maps", 2, dataChunks.size());

        Set<String> allMapNames = dataChunks.stream()
                .map(chunk -> chunk)
                .flatMap(chunk -> chunk.getServiceData().stream())
                .map(KeyValueDataChunk::getName)
                .collect(Collectors.toSet());

        assertTrue("Both maps should be present",
                allMapNames.containsAll(List.of("small_map", "large_map")));
    }

    @Test
    public void testResetForChunkedSnapshotRestore_WithMultipleMaps() {
        // Add multiple maps and destroyed names
        cpMapRegistry.getMapStores().put("map1", new HeapCPMapStore(100));
        cpMapRegistry.getMapStores().put("map2", new HeapCPMapStore(100));
        cpMapRegistry.getMapStores().put("map3", new HeapCPMapStore(100));
        cpMapRegistry.getDestroyed().add("destroyed-map1");
        cpMapRegistry.getDestroyed().add("destroyed-map2");

        // Perform reset
        cpMapRegistry.resetForChunkedSnapshotRestore();

        assertTrue("MapStores should be empty after reset with multiple maps",
                cpMapRegistrySnapshot.getMapStores().isEmpty());
        assertTrue("Destroyed should be empty after reset with multiple destroyed maps",
                cpMapRegistrySnapshot.getDestroyed().isEmpty());
    }


    private CPMapStore createMockMapStore(int entryCount) {
        CPMapStore mockStore = mock(CPMapStore.class);
        List<Map.Entry<Data, Data>> entries = new ArrayList<>();

        for (int i = 0; i < entryCount; i++) {
            entries.add(new AbstractMap.SimpleEntry<>(mockKey, mockValue));
        }

        Iterator<Map.Entry<Data, Data>> iterator = entries.iterator();
        when(mockStore.iterator()).thenReturn(iterator);
        when(mockStore.size()).thenReturn(entryCount);

        return mockStore;
    }
}
