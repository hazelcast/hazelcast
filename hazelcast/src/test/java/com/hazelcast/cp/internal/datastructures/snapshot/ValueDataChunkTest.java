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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.ByteArrayObjectDataOutput;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ValueDataChunkTest {

    private BufferObjectDataOutput objectDataOutput;
    private BufferObjectDataInput objectDataInput;
    private byte[] bytes;

    @Before
    public void setup() {
        objectDataOutput = mock(ByteArrayObjectDataOutput.class);
        objectDataInput = mock(BufferObjectDataInput.class);

        bytes = new byte[16];
        Random random = new Random();
        random.nextBytes(bytes);
    }

    @Test
    public void testConstructorWithValue() {
        String name = "testValue";
        Data value = new HeapData(bytes);

        ValueDataChunk valueDataChunk = new ValueDataChunk(name, value);

        assertEquals(name, valueDataChunk.getName());
        assertEquals(value, valueDataChunk.getValue());
        assertFalse(valueDataChunk.isDestroyed());
    }

    @Test
    public void testConstructorWithDestroyed() {
        String name = "testValue";
        boolean destroyed = true;

        ValueDataChunk valueDataChunk = new ValueDataChunk(name, destroyed);

        assertEquals(name, valueDataChunk.getName());
        assertNull(valueDataChunk.getValue());
        assertTrue(valueDataChunk.isDestroyed());
    }

    @Test
    public void testDefaultConstructor() {
        ValueDataChunk valueDataChunk = new ValueDataChunk();

        assertNull(valueDataChunk.getName());
        assertNull(valueDataChunk.getValue());
        assertFalse(valueDataChunk.isDestroyed());
    }

    @Test
    public void testWriteDataNotDestroyed() throws IOException {
        String name = "testValue";
        Data value = new HeapData(bytes);

        ValueDataChunk valueDataChunk = new ValueDataChunk(name, value);

        valueDataChunk.writeData(objectDataOutput);

        verify(objectDataOutput).writeString(name);
        verify(objectDataOutput).writeBoolean(false);
        verify(objectDataOutput).writeData(value);
    }

    @Test
    public void testWriteDataDestroyed() throws IOException {
        String name = "testValue";
        ValueDataChunk valueDataChunk = new ValueDataChunk(name, true);

        valueDataChunk.writeData(objectDataOutput);

        verify(objectDataOutput).writeString(name);
        verify(objectDataOutput).writeBoolean(true);
        verify(objectDataOutput, Mockito.never()).writeData(any(Data.class));
    }

    @Test
    public void testReadDataNotDestroyed() throws IOException {
        String name = "testValue";
        Data value = new HeapData(bytes);

        when(objectDataInput.readString()).thenReturn(name);
        when(objectDataInput.readBoolean()).thenReturn(false);
        when(objectDataInput.readData()).thenReturn(value);

        ValueDataChunk valueDataChunk = new ValueDataChunk();
        valueDataChunk.readData(objectDataInput);

        assertEquals(name, valueDataChunk.getName());
        assertEquals(value, valueDataChunk.getValue());
        assertFalse(valueDataChunk.isDestroyed());
    }

    @Test
    public void testReadDataDestroyed() throws IOException {
        String name = "testValue";

        when(objectDataInput.readString()).thenReturn(name);
        when(objectDataInput.readBoolean()).thenReturn(true);

        ValueDataChunk valueDataChunk = new ValueDataChunk();
        valueDataChunk.readData(objectDataInput);

        assertEquals(name, valueDataChunk.getName());
        assertNull(valueDataChunk.getValue());
        assertTrue(valueDataChunk.isDestroyed());
        verify(objectDataInput, times(1)).readData();
    }
}
