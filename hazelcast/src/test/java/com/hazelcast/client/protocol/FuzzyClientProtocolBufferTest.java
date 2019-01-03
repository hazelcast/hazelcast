/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.util.ClientProtocolBuffer;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.client.impl.protocol.util.UnsafeBuffer;
import com.hazelcast.nio.Bits;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.Charset;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class FuzzyClientProtocolBufferTest extends HazelcastTestSupport {

    private static ClientProtocolBuffer safeBuffer;
    private static ClientProtocolBuffer unsafeBuffer;
    private static final Random random = new Random();
    private static int capacity;

    @BeforeClass
    public static void init() {
        capacity = random.nextInt(1000) + 1000;
        safeBuffer = new SafeBuffer(new byte[capacity]);
        unsafeBuffer = new UnsafeBuffer(new byte[capacity]);
    }

    @Test
    public void testBasics_withSafeBuffer() {
        testBasics(safeBuffer);
    }

    @Test
    public void testBasics_withUnSafeBuffer() {
        testBasics(unsafeBuffer);
    }

    private void testBasics(ClientProtocolBuffer clientProtocolBuffer) {
        testCapacity(clientProtocolBuffer);
        testInt(clientProtocolBuffer);
        testShort(clientProtocolBuffer);
        testLong(clientProtocolBuffer);
        testByte(clientProtocolBuffer);
        testBytes(clientProtocolBuffer);
        testString(clientProtocolBuffer);
        testWrapAndGet(clientProtocolBuffer);
    }

    private void testCapacity(ClientProtocolBuffer clientProtocolBuffer) {
        assertEquals(capacity, clientProtocolBuffer.capacity());
    }

    private void testShort(ClientProtocolBuffer clientProtocolBuffer) {
        int index = pickIndexSuitableForSize(Bits.SHORT_SIZE_IN_BYTES);
        short expected = (short) random.nextInt();
        clientProtocolBuffer.putShort(index, expected);
        assertEquals(expected, clientProtocolBuffer.getShort(index));
    }

    private void testString(ClientProtocolBuffer clientProtocolBuffer) {
        String expected = randomString();
        int index = pickIndexSuitableForSize(expected.getBytes(Charset.forName("utf-8")).length + Bits.INT_SIZE_IN_BYTES);
        clientProtocolBuffer.putStringUtf8(index, expected);
        int strSize = clientProtocolBuffer.getInt(index);
        assertEquals(expected, clientProtocolBuffer.getStringUtf8(index, strSize));
    }

    private void testWrapAndGet(ClientProtocolBuffer clientProtocolBuffer) {
        byte[] expected = new byte[capacity];
        random.nextBytes(expected);
        clientProtocolBuffer.wrap(expected);
        assertArrayEquals(expected, clientProtocolBuffer.byteArray());
    }

    private void testInt(ClientProtocolBuffer clientProtocolBuffer) {
        int index = pickIndexSuitableForSize(Bits.INT_SIZE_IN_BYTES);
        int expected = random.nextInt();
        clientProtocolBuffer.putInt(index, expected);
        assertEquals(expected, clientProtocolBuffer.getInt(index));
    }

    private void testLong(ClientProtocolBuffer clientProtocolBuffer) {
        int index = pickIndexSuitableForSize(Bits.LONG_SIZE_IN_BYTES);
        long expected = random.nextLong();
        clientProtocolBuffer.putLong(index, expected);
        assertEquals(expected, clientProtocolBuffer.getLong(index));
    }

    private void testByte(ClientProtocolBuffer clientProtocolBuffer) {
        int index = pickIndexSuitableForSize(Bits.BYTE_SIZE_IN_BYTES);
        byte expected = (byte) random.nextInt();
        clientProtocolBuffer.putByte(index, expected);
        assertEquals(expected, clientProtocolBuffer.getByte(index));
    }

    private void testBytes(ClientProtocolBuffer clientProtocolBuffer) {
        int bytesSize = random.nextInt(capacity);
        byte[] expected = new byte[bytesSize];
        random.nextBytes(expected);
        int index = pickIndexSuitableForSize(bytesSize);
        clientProtocolBuffer.putBytes(index, expected);
        byte[] actual = new byte[bytesSize];
        clientProtocolBuffer.getBytes(index, actual);
        assertArrayEquals(expected, actual);
    }

    private int pickIndexSuitableForSize(int size) {
        return random.nextInt(capacity - size);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIndexOutOfBoundException_withSafeBuffer() {
        testIndexOutOfBoundException(safeBuffer);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIndexOutOfBoundException_withUnsafeBuffer() {
        testIndexOutOfBoundException(unsafeBuffer);
    }

    private void testIndexOutOfBoundException(ClientProtocolBuffer clientProtocolBuffer) {
        clientProtocolBuffer.getInt(capacity + 1);
    }
}
