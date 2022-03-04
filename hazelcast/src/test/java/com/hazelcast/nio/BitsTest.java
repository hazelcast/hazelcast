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

package com.hazelcast.nio;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BitsTest {

    private static Random random = new Random();
    private static byte[] readBuffer;

    @BeforeClass
    public static void initBuffer() {
        readBuffer = new byte[8];
        random.nextBytes(readBuffer);
    }

    @Test
    public void testReadCharBigEndian() {
        testReadChar(true);
    }

    @Test
    public void testReadCharLittleEndian() {
        testReadChar(false);
    }

    private void testReadChar(boolean bigEndian) {
        char ch1 = Bits.readChar(readBuffer, 0, bigEndian);
        ByteBuffer buffer = getByteBuffer(bigEndian);
        char ch2 = buffer.getChar();

        assertEquals(ch2, ch1);
    }

    @Test
    public void testReadShortBigEndian() {
        testReadShort(true);
    }

    @Test
    public void testReadShortLittleEndian() {
        testReadShort(false);
    }

    private void testReadShort(boolean bigEndian) {
        short s1 = Bits.readShort(readBuffer, 0, bigEndian);
        ByteBuffer buffer = getByteBuffer(bigEndian);
        short s2 = buffer.getShort();

        assertEquals(s2, s1);
    }

    @Test
    public void testReadIntBigEndian() {
        testReadInt(true);
    }

    @Test
    public void testReadIntLittleEndian() {
        testReadInt(false);
    }

    private void testReadInt(boolean bigEndian) {
        int i1 = Bits.readInt(readBuffer, 0, bigEndian);
        ByteBuffer buffer = getByteBuffer(bigEndian);
        int i2 = buffer.getInt();

        assertEquals(i2, i1);
    }

    @Test
    public void testReadLongBigEndian() {
        testReadLong(true);
    }

    @Test
    public void testReadLongLittleEndian() {
        testReadLong(false);
    }

    private void testReadLong(boolean bigEndian) {
        long l1 = Bits.readLong(readBuffer, 0, bigEndian);
        ByteBuffer buffer = getByteBuffer(bigEndian);
        long l2 = buffer.getLong();

        assertEquals(l2, l1);
    }

    private static ByteBuffer getByteBuffer(boolean bigEndian) {
        return ByteBuffer.wrap(BitsTest.readBuffer)
                .order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    public void testWriteCharBigEndian() {
        testWriteChar(true);
    }

    @Test
    public void testWriteCharLittleEndian() {
        testWriteChar(false);
    }

    private void testWriteChar(boolean bigEndian) {
        char c = (char) random.nextInt();

        byte[] bb = new byte[2];
        Bits.writeChar(bb, 0, c, bigEndian);

        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        byteBuffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putChar(c);

        assertArrayEquals(byteBuffer.array(), bb);
    }

    @Test
    public void testWriteShortBigEndian() {
        testWriteShort(true);
    }

    @Test
    public void testWriteShortLittleEndian() {
        testWriteShort(false);
    }

    private void testWriteShort(boolean bigEndian) {
        short c = (short) random.nextInt();

        byte[] bb = new byte[2];
        Bits.writeShort(bb, 0, c, bigEndian);

        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        byteBuffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putShort(c);

        assertArrayEquals(byteBuffer.array(), bb);
    }

    @Test
    public void testWriteIntBigEndian() {
        testWriteInt(true);
    }

    @Test
    public void testWriteIntLittleEndian() {
        testWriteInt(false);
    }

    private void testWriteInt(boolean bigEndian) {
        int c = random.nextInt();

        byte[] bb = new byte[4];
        Bits.writeInt(bb, 0, c, bigEndian);

        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(c);

        assertArrayEquals(byteBuffer.array(), bb);
    }

    @Test
    public void testWriteLongBigEndian() {
        testWriteLong(true);
    }

    @Test
    public void testWriteLongLittleEndian() {
        testWriteLong(false);
    }

    private void testWriteLong(boolean bigEndian) {
        long c = random.nextLong();

        byte[] bb = new byte[8];
        Bits.writeLong(bb, 0, c, bigEndian);

        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putLong(c);

        assertArrayEquals(byteBuffer.array(), bb);
    }

    @Test
    public void testWriteUtf8Char() {
        byte[] bytes = new byte[3];
        char c1 = 0x0010; //1 byte
        char c2 = 0x0080; //2 byte
        char c3 = 0x0800; //3 byte
        assertEquals(1, Bits.writeUtf8Char(bytes, 0, c1));
        assertEquals(2, Bits.writeUtf8Char(bytes, 0, c2));
        assertEquals(3, Bits.writeUtf8Char(bytes, 0, c3));
    }

    @Test
    public void testSetBitByte() throws Exception {
        byte b = 110;
        b = Bits.setBit(b, 0);
        assertEquals(111, b);

        b = Bits.setBit(b, 7);
        assertTrue(b < 0);
    }

    @Test
    public void testClearBitByte() throws Exception {
        byte b = 111;
        b = Bits.clearBit(b, 0);
        assertEquals(110, b);

        b = -111;
        b = Bits.clearBit(b, 7);
        assertTrue(b > 0);
    }

    @Test
    public void testInvertBitByte() throws Exception {
        byte b = -111;
        b = Bits.invertBit(b, 7);
        assertTrue(b > 0);
    }

    @Test
    public void testSetBitInteger() throws Exception {
        int b = 110;
        b = Bits.setBit(b, 0);
        assertEquals(111, b);

        b = Bits.setBit(b, 31);
        assertTrue(b < 0);
    }

    @Test
    public void testClearBitInteger() throws Exception {
        int b = 111;
        b = Bits.clearBit(b, 0);
        assertEquals(110, b);

        b = -111;
        b = Bits.clearBit(b, 31);
        assertTrue(b > 0);
    }

    @Test
    public void testInvertBitInteger() throws Exception {
        int b = -111111;
        b = Bits.invertBit(b, 31);
        assertTrue(b > 0);
    }

    @Test
    public void testIsBitSet() throws Exception {
        assertFalse(Bits.isBitSet(123, 31));
        assertTrue(Bits.isBitSet(-123, 31));
        assertFalse(Bits.isBitSet(222, 0));
        assertTrue(Bits.isBitSet(221, 0));
    }

    @Test
    public void testCombineToInt() throws Exception {
        short x = (short) random.nextInt();
        short y = (short) random.nextInt();

        int k = Bits.combineToInt(x, y);
        assertEquals(x, Bits.extractShort(k, false));
        assertEquals(y, Bits.extractShort(k, true));
    }

    @Test
    public void testCombineToLong() throws Exception {
        int x = random.nextInt();
        int y = random.nextInt();

        long k = Bits.combineToLong(x, y);
        assertEquals(x, Bits.extractInt(k, false));
        assertEquals(y, Bits.extractInt(k, true));
    }
}
