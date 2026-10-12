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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V5_7;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class KeyValueDataChunkTest {

    private BufferObjectDataOutput out;
    private BufferObjectDataInput in;

    @Before
    public void setUp() {
        out = mock(BufferObjectDataOutput.class);
        in = mock(BufferObjectDataInput.class);

        Version version = V5_7;
        when(out.getVersion()).thenReturn(version);
        when(in.getVersion()).thenReturn(version);
    }

    @Test
    public void constructor_shouldInitializeEmptyState() {
        KeyValueDataChunk chunk = new KeyValueDataChunk();

        assertNull(chunk.getName());
        assertTrue(chunk.getKeyValuePairs().isEmpty());
        assertTrue(chunk.getEntryTimestamps().isEmpty());
        assertFalse(chunk.isDestroyed());
    }

    @Test
    public void constructor_withNameAndDestroyed_shouldSetState() {
        KeyValueDataChunk chunk = new KeyValueDataChunk("map", true);

        assertEquals("map", chunk.getName());
        assertTrue(chunk.isDestroyed());
        assertTrue(chunk.getKeyValuePairs().isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void constructor_shouldRejectNullName() {
        new KeyValueDataChunk(null, false);
    }

    @Test
    public void add_withoutTimestamp_shouldNotPopulateTimestamps() {
        Data key = mockData(10);
        Data value = mockData(20);

        KeyValueDataChunk chunk = new KeyValueDataChunk("map");

        long initialSize = chunk.getChunkSizeInBytes();

        chunk.add(key, value, 1000, 10);

        // single entry occupies 2 slot. 1 for key and 1 for value
        assertEquals(2, chunk.getKeyValuePairs().size());
        assertTrue(chunk.getEntryTimestamps().isEmpty());
        assertEquals(initialSize + 30, chunk.getChunkSizeInBytes());
    }

    @Test
    public void add_withTimestamp_shouldPopulateTimestampsAndSize() {
        Data key = mockData(10);
        Data value = mockData(20);

        KeyValueDataChunk chunk = new KeyValueDataChunk("map");

        long initialSize = chunk.getChunkSizeInBytes();

        chunk.add(key, value, 123L, 1000, 10);

        // single entry occupies 2 slot. 1 for key and 1 for value
        assertEquals(2, chunk.getKeyValuePairs().size());
        assertEquals(1, chunk.getEntryTimestamps().size());
        assertEquals(Long.valueOf(123L), chunk.getEntryTimestamps().get(0));

        assertEquals(
                initialSize + 30 + Bits.LONG_SIZE_IN_BYTES,
                chunk.getChunkSizeInBytes()
        );
    }

    @Test
    public void writeData_shouldWriteBasicFields() throws IOException {
        KeyValueDataChunk chunk = new KeyValueDataChunk("map", true);

        chunk.writeData(out);

        verify(out).writeString("map");
        verify(out).writeBoolean(true);
        verify(out).writeLong(chunk.getChunkSizeInBytes());
        verify(out, times(2)).writeInt(anyInt());
    }

    @Test
    public void readData_shouldDeserializeKeyValues() throws IOException {
        when(in.readString()).thenReturn("map");
        when(in.readBoolean()).thenReturn(false);
        when(in.readInt()).thenReturn(1); // kv count
        when(in.readData()).thenReturn(mock(Data.class));

        KeyValueDataChunk chunk = new KeyValueDataChunk();
        chunk.readData(in);

        assertEquals("map", chunk.getName());
        assertFalse(chunk.isDestroyed());
        assertEquals(1, chunk.getKeyValuePairs().size());
    }

    @Test
    public void readData_whenDestroyed_shouldSkipKeyValues() throws IOException {
        when(in.readString()).thenReturn("map");
        when(in.readBoolean()).thenReturn(true);

        KeyValueDataChunk chunk = new KeyValueDataChunk();
        chunk.readData(in);

        assertTrue(chunk.isDestroyed());
        assertTrue(chunk.getKeyValuePairs().isEmpty());

        verify(in, times(2)).readInt();
    }

    @Test
    public void readData_shouldDeserializeTimestampsWhenSupported() throws IOException {
        when(in.readString()).thenReturn("map");
        when(in.readBoolean()).thenReturn(false);

        when(in.readLong()).thenReturn(123L, 42L);
        when(in.readInt()).thenReturn(1, 1);
        when(in.readData()).thenReturn(mock(Data.class));

        KeyValueDataChunk chunk = new KeyValueDataChunk();
        chunk.readData(in);

        assertEquals(1, chunk.getEntryTimestamps().size());
        assertEquals(Long.valueOf(42L), chunk.getEntryTimestamps().get(0));
    }

    @Test
    public void equals_shouldMatchEmptyDeserializedChunk() throws IOException {
        when(in.readString()).thenReturn("map");
        when(in.readBoolean()).thenReturn(false);
        when(in.readLong()).thenReturn(12L);
        when(in.readInt()).thenReturn(0, 0);

        KeyValueDataChunk actual = new KeyValueDataChunk();
        actual.readData(in);

        KeyValueDataChunk expected = new KeyValueDataChunk("map", false);

        assertEquals(expected, actual);
        assertEquals(expected.hashCode(), actual.hashCode());
    }

    @Test
    public void toString_shouldHandleEmptyState() {
        KeyValueDataChunk chunk = new KeyValueDataChunk();

        String str = chunk.toString();

        assertTrue(str.contains("keyValuePairs.size()=0"));
        assertTrue(str.contains("entryTimestamps.size()=0"));
    }

    private static Data mockData(int size) {
        Data data = mock(Data.class);
        when(data.dataSize()).thenReturn(size);
        return data;
    }
}
