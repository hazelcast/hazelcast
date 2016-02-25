/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.memory.impl;

import com.hazelcast.internal.memory.MemoryAccessStrategy;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

@SuppressWarnings({"checkstyle:methodcount"})
public class ByteBufferMemoryAccessStrategy implements MemoryAccessStrategy<byte[]> {
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    private void validateObject(Object srcObj) {
        assert srcObj != null : "srcObj should not  be null";
    }

    private void checkOffset(long offset) {
        assert offset >= 0 && offset <= Integer.MAX_VALUE : "invalid offset: " + offset;
    }

    @Override
    public long objectFieldOffset(Field field) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public int arrayBaseOffset(Class<?> arrayClass) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public int arrayIndexScale(Class<?> arrayClass) {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void copyMemory(byte[] srcObj, long srcOffset, byte[] destObj, long destOffset, long bytes) {
        validateObject(srcObj);
        validateObject(destObj);
        checkOffset(srcOffset);
        checkOffset(destOffset);
        checkOffset(bytes);
        System.arraycopy(srcObj, (int) srcOffset, destObj, (int) destOffset, (int) bytes);
    }


    @Override
    public boolean getBoolean(byte[] array, long offset) {
        validateObject(array);
        checkOffset(offset);
        return array[(int) offset] != 0;
    }

    @Override
    public boolean getBooleanVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putBoolean(byte[] array, long offset, boolean x) {
        validateObject(array);
        checkOffset(offset);
        array[(int) offset] = x ? (byte) 1 : 0;
    }

    @Override
    public void putBooleanVolatile(byte[] array, long offset, boolean x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public byte getByte(byte[] array, long offset) {
        validateObject(array);
        checkOffset(offset);
        return array[(int) offset];
    }

    @Override
    public byte getByteVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putByte(byte[] array, long offset, byte x) {
        validateObject(array);
        checkOffset(offset);
        array[(int) offset] = x;
    }

    @Override
    public void putByteVolatile(byte[] array, long offset, byte x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public char getChar(byte[] array, long offset) {
        validateObject(array);
        checkOffset(offset);
        return EndiannessUtility.readChar(this, array, (int) offset, BIG_ENDIAN);
    }

    @Override
    public char getCharVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putChar(byte[] array, long offset, char x) {
        validateObject(array);
        checkOffset(offset);
        EndiannessUtility.writeChar(this, array, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putCharVolatile(byte[] array, long offset, char x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public short getShort(byte[] array, long offset) {
        validateObject(array);
        checkOffset(offset);
        return EndiannessUtility.readShort(array, (int) offset, BIG_ENDIAN);
    }

    @Override
    public short getShortVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putShort(byte[] array, long offset, short x) {
        validateObject(array);
        checkOffset(offset);
        EndiannessUtility.writeShort(array, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putShortVolatile(byte[] array, long offset, short x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int getInt(byte[] array, long offset) {
        validateObject(array);
        checkOffset(offset);
        return EndiannessUtility.readInt(array, (int) offset, BIG_ENDIAN);
    }

    @Override
    public int getIntVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putInt(byte[] array, long offset, int x) {
        validateObject(array);
        checkOffset(offset);
        EndiannessUtility.writeInt(array, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putIntVolatile(byte[] array, long offset, int x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public float getFloat(byte[] array, long offset) {
        validateObject(array);
        checkOffset(offset);
        return EndiannessUtility.readFloat(array, (int) offset, BIG_ENDIAN);
    }

    @Override
    public float getFloatVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putFloat(byte[] array, long offset, float x) {
        validateObject(array);
        checkOffset(offset);
        EndiannessUtility.writeFloat(array, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putFloatVolatile(byte[] array, long offset, float x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public long getLong(byte[] array, long offset) {
        validateObject(array);
        checkOffset(offset);
        return EndiannessUtility.readLong(array, (int) offset, BIG_ENDIAN);
    }

    @Override
    public long getLongVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putLong(byte[] array, long offset, long x) {
        validateObject(array);
        checkOffset(offset);
        EndiannessUtility.writeLong(array, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putLongVolatile(byte[] array, long offset, long x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public double getDouble(byte[] array, long offset) {
        validateObject(array);
        checkOffset(offset);
        return EndiannessUtility.readDouble(array, (int) offset, BIG_ENDIAN);
    }

    @Override
    public double getDoubleVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putDouble(byte[] array, long offset, double x) {
        validateObject(array);
        checkOffset(offset);
        EndiannessUtility.writeDouble(array, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putDoubleVolatile(byte[] array, long offset, double x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object getObject(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object getObjectVolatile(byte[] array, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putObject(byte[] array, long offset, Object x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putObjectVolatile(byte[] array, long offset, Object x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean compareAndSwapInt(byte[] array, long offset, int expected, int x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean compareAndSwapLong(byte[] array, long offset, long expected, long x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean compareAndSwapObject(byte[] array, long offset, Object expected, Object x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putOrderedInt(byte[] array, long offset, int x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putOrderedLong(byte[] array, long offset, long x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putOrderedObject(byte[] array, long offset, Object x) {
        throw new UnsupportedOperationException("Not supported");
    }
}
