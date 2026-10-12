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

package com.hazelcast.cp.internal.datastructures.snapshot;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants.DATA_CHUNK_GROUP;
import static com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants.F_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
class DataChunkGroupTest {

    private DataChunkGroup<String> chunkGroup;
    private BufferObjectDataOutput output;
    private BufferObjectDataInput input;

    @BeforeEach
    void setUp() {
        chunkGroup = new DataChunkGroup<>();
        output = mock(BufferObjectDataOutput.class);
        input = mock(BufferObjectDataInput.class);
    }

    @Test
    void testConstructor_initializesEmptyList() {
        assertTrue(chunkGroup.getServiceData().isEmpty(), "Data list should be empty on initialization");
        assertNotNull(chunkGroup.getServiceData(), "Data list should not be null");
    }

    @Test
    void testAdd_nonNullItem_addsToList() {
        String item = "testItem";
        chunkGroup.add(item);
        List<String> data = chunkGroup.getServiceData();
        assertEquals(1, data.size(), "List should contain one item");
        assertEquals(item, data.get(0), "Added item should match");
    }

    @Test
    void testAdd_nullItem_throwsNullPointerException() {
        NullPointerException exception = assertThrows(NullPointerException.class,
                () -> chunkGroup.add(null),
                "Expected NullPointerException for null item");
        assertEquals("Item cannot be null", exception.getMessage());
    }

    @Test
    void testAdd_multipleItems_addsInOrder() {
        chunkGroup.add("item1").add("item2");
        List<String> data = chunkGroup.getServiceData();
        assertEquals(2, data.size(), "List should contain two items");
        assertEquals("item1", data.get(0), "First item should match");
        assertEquals("item2", data.get(1), "Second item should match");
    }

    @Test
    void testIsEmpty_emptyChunk_returnsTrue() {
        assertTrue(chunkGroup.isEmpty(), "Empty chunk should return true");
    }

    @Test
    void testIsEmpty_nonEmptyChunk_returnsFalse() {
        chunkGroup.add("item");
        assertFalse(chunkGroup.isEmpty(), "Non-empty chunk should return false");
    }

    @Test
    void testGetFactoryId_returnsCorrectId() {
        assertEquals(F_ID, chunkGroup.getFactoryId(), "Factory ID should match RaftDataSerializerConstants.F_ID");
    }

    @Test
    void testGetClassId_returnsCorrectId() {
        assertEquals(DATA_CHUNK_GROUP, chunkGroup.getClassId(), "Class ID should match RaftDataSerializerConstants.DATA_CHUNK_GROUP");
    }

    @Test
    void testWriteData_writesSizeAndItems() throws IOException {
        chunkGroup.add("item1").add("item2");
        chunkGroup.writeData(output);

        verify(output).writeInt(2);
        verify(output).writeObject("item1");
        verify(output).writeObject("item2");
        verifyNoMoreInteractions(output);
    }

    @Test
    void testWriteData_emptyChunk_writesZeroSize() throws IOException {
        chunkGroup.writeData(output);

        verify(output).writeInt(0);
        verifyNoMoreInteractions(output);
    }

    @Test
    void testReadData_readsSizeAndItems() throws IOException {
        when(input.readInt()).thenReturn(2);
        when(input.readObject()).thenReturn("item1", "item2");

        chunkGroup.readData(input);

        List<String> data = chunkGroup.getServiceData();
        assertEquals(2, data.size(), "List should contain two items");
        assertEquals("item1", data.get(0), "First item should match");
        assertEquals("item2", data.get(1), "Second item should match");
        verify(input).readInt();
        verify(input, times(2)).readObject();
        verifyNoMoreInteractions(input);
    }

    @Test
    void testReadData_emptyChunk_readsZeroSize() throws IOException {
        when(input.readInt()).thenReturn(0);

        chunkGroup.readData(input);

        assertTrue(chunkGroup.isEmpty(), "Chunk should be empty after reading zero size");
        verify(input).readInt();
        verifyNoMoreInteractions(input);
    }

    @Test
    void testToString_emptyChunk_returnsEmptyData() {
        assertEquals("Chunk{data=[]}", chunkGroup.toString(),
                "toString should reflect empty data");
    }

    @Test
    void testToString_nonEmptyChunk_returnsData() {
        chunkGroup.add("item1").add("item2");
        assertEquals("Chunk{data=[item1, item2]}", chunkGroup.toString(),
                "toString should reflect data content");
    }

    @Test
    void testSerialization_cycle_preservesData() throws IOException {
        chunkGroup.add("item1").add("item2");

        // Simulate write
        doAnswer(invocation -> {
            chunkGroup.writeData(output);
            return null;
        }).when(output).writeObject(any());

        // Simulate read
        when(input.readInt()).thenReturn(2);
        when(input.readObject()).thenReturn("item1", "item2");

        DataChunkGroup<String> newChunk = new DataChunkGroup<>();
        newChunk.readData(input);

        assertEquals(chunkGroup.getServiceData(), newChunk.getServiceData(),
                "Data should be preserved after serialization cycle");
    }

    @Test
    void testGenericType_worksWithDifferentTypes() {
        DataChunkGroup<Integer> intChunk = new DataChunkGroup<>();
        intChunk.add(42).add(100);
        List<Integer> data = intChunk.getServiceData();
        assertEquals(2, data.size(), "List should contain two integers");
        assertEquals(Integer.valueOf(42), data.get(0), "First integer should match");
        assertEquals(Integer.valueOf(100), data.get(1), "Second integer should match");
    }
}
