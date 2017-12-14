/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;


import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.client.impl.protocol.util.UnsafeBuffer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.EOFException;
import java.nio.ByteOrder;

import static org.mockito.Mockito.mock;

/**
 * Bounds check test various input/output classes that are susceptible to bypassing with integer wrapping
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BoundsCheckTest {

    @Test(expected = IndexOutOfBoundsException.class)
    public void testUnsafeBufferBoundsTooBig() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[100]);
        byte[] src = new byte[10];
        buffer.putBytes(80, src, 0, Integer.MAX_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testUnsafeBufferBoundsTooSmall() {
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[100]);
        byte[] src = new byte[10];
        buffer.putBytes(80, src, 0, Integer.MIN_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSafeBufferBoundsTooBig() {
        SafeBuffer buffer = new SafeBuffer(new byte[100]);
        byte[] src = new byte[10];
        buffer.putBytes(80, src, 10, Integer.MAX_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSafeBufferBoundsTooSmall() {
        SafeBuffer buffer = new SafeBuffer(new byte[100]);
        byte[] src = new byte[10];
        buffer.putBytes(80, src, 10, Integer.MIN_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testUnsafeObjectDataOutputBoundsTooBig() {
        byte[] src = new byte[10];
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        UnsafeObjectDataOutput dataOutput = new UnsafeObjectDataOutput(100, mockSerializationService);
        dataOutput.write(src, 10, Integer.MAX_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testUnsafeObjectDataOutputBoundsTooSmall() {
        byte[] src = new byte[10];
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        UnsafeObjectDataOutput dataOutput = new UnsafeObjectDataOutput(100, mockSerializationService);
        dataOutput.write(src, 10, Integer.MIN_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testByteArrayObjectDataOutputBoundsTooBig() {
        byte[] src = new byte[10];
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        ByteArrayObjectDataOutput dataOutput = new ByteArrayObjectDataOutput(100, mockSerializationService, ByteOrder.BIG_ENDIAN);
        dataOutput.write(src, 10, Integer.MAX_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testByteArrayObjectDataOutputBoundsTooSmall() {
        byte[] src = new byte[10];
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        ByteArrayObjectDataOutput dataOutput = new ByteArrayObjectDataOutput(100, mockSerializationService, ByteOrder.BIG_ENDIAN);
        dataOutput.write(src, 10, Integer.MIN_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testByteArrayObjectDataInputBoundsTooBig() throws EOFException {
        byte[] data = new byte[100];
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        ByteArrayObjectDataInput dataInput = new ByteArrayObjectDataInput(data, mockSerializationService, ByteOrder.BIG_ENDIAN);
        byte[] dest = new byte[10];
        dataInput.read(dest, 10, Integer.MAX_VALUE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testByteArrayObjectDataInputBoundsTooSMall() throws EOFException {
        byte[] data = new byte[10];
        InternalSerializationService mockSerializationService = mock(InternalSerializationService.class);
        ByteArrayObjectDataInput dataInput = new ByteArrayObjectDataInput(data, mockSerializationService, ByteOrder.BIG_ENDIAN);
        byte[] dest = new byte[10];
        dataInput.read(dest, 10, Integer.MIN_VALUE);
    }
}
