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

import com.hazelcast.internal.memory.MemoryAccessor;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

public class ByteBufferMemoryAccessor implements MemoryAccessor {
    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    private void validateObject(Object srcObj) {
        assert srcObj != null : "srcObj should not  be null";
        assert srcObj instanceof byte[] : "srcObj should be array of bytes";
    }

    private void checkOffset(long offset) {
        assert offset >= 0 && offset <= Integer.MAX_VALUE : "invalid offset: " + offset;
    }

    @Override
    public long objectFieldOffset(Field field) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int arrayBaseOffset(Class<?> arrayClass) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int arrayIndexScale(Class<?> arrayClass) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void copyMemory(long srcAddress, long destAddress, long bytes) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void copyMemory(Object srcObj, long srcOffset, Object destObj, long destOffset, long bytes) {
        validateObject(srcObj);
        validateObject(destObj);
        checkOffset(srcOffset);
        checkOffset(destOffset);
        checkOffset(bytes);
        System.arraycopy(srcObj, (int) srcOffset, destObj, (int) destOffset, (int) bytes);
    }

    @Override
    public void setMemory(long address, long bytes, byte value) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean getBoolean(long address) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean getBoolean(Object o, long offset) {
        validateObject(o);
        checkOffset(offset);
        return ((byte[]) o)[(int) offset] != 0;
    }

    @Override
    public boolean getBooleanVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putBoolean(long address, boolean x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putBoolean(Object o, long offset, boolean x) {
        validateObject(o);
        checkOffset(offset);
        ((byte[]) o)[(int) offset] = x ? (byte) 1 : 0;
    }

    @Override
    public void putBooleanVolatile(Object o, long offset, boolean x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public byte getByte(long address) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public byte getByte(Object o, long offset) {
        validateObject(o);
        checkOffset(offset);
        return ((byte[]) o)[(int) offset];
    }

    @Override
    public byte getByteVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putByte(long address, byte x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putByte(Object o, long offset, byte x) {
        validateObject(o);
        checkOffset(offset);
        ((byte[]) o)[(int) offset] = x;
    }

    @Override
    public void putByteVolatile(Object o, long offset, byte x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public char getChar(long address) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public char getChar(Object o, long offset) {
        validateObject(o);
        checkOffset(offset);
        return DirectMemoryBits.readChar(this, o, (int) offset, BIG_ENDIAN);
    }

    @Override
    public char getCharVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putChar(long address, char x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putChar(Object o, long offset, char x) {
        validateObject(o);
        checkOffset(offset);
        DirectMemoryBits.writeChar(this, o, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putCharVolatile(Object o, long offset, char x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public short getShort(long address) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public short getShort(Object o, long offset) {
        validateObject(o);
        checkOffset(offset);
        return DirectMemoryBits.readShort((byte[]) o, (int) offset, BIG_ENDIAN);
    }

    @Override
    public short getShortVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putShort(long address, short x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putShort(Object o, long offset, short x) {
        validateObject(o);
        checkOffset(offset);
        DirectMemoryBits.writeShort((byte[]) o, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putShortVolatile(Object o, long offset, short x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int getInt(long address) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int getInt(Object o, long offset) {
        validateObject(o);
        checkOffset(offset);
        return DirectMemoryBits.readInt((byte[]) o, (int) offset, BIG_ENDIAN);
    }

    @Override
    public int getIntVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putInt(long address, int x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putInt(Object o, long offset, int x) {
        validateObject(o);
        checkOffset(offset);
        DirectMemoryBits.writeInt((byte[]) o, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putIntVolatile(Object o, long offset, int x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public float getFloat(long address) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public float getFloat(Object o, long offset) {
        validateObject(o);
        checkOffset(offset);
        return DirectMemoryBits.readFloat((byte[]) o, (int) offset, BIG_ENDIAN);
    }

    @Override
    public float getFloatVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putFloat(long address, float x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putFloat(Object o, long offset, float x) {
        validateObject(o);
        checkOffset(offset);
        DirectMemoryBits.writeFloat((byte[]) o, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putFloatVolatile(Object o, long offset, float x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public long getLong(long address) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public long getLong(Object o, long offset) {
        validateObject(o);
        checkOffset(offset);
        return DirectMemoryBits.readLong((byte[]) o, (int) offset, BIG_ENDIAN);
    }

    @Override
    public long getLongVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putLong(long address, long x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putLong(Object o, long offset, long x) {
        validateObject(o);
        checkOffset(offset);
        DirectMemoryBits.writeLong((byte[]) o, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putLongVolatile(Object o, long offset, long x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public double getDouble(long address) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public double getDouble(Object o, long offset) {
        validateObject(o);
        checkOffset(offset);
        return DirectMemoryBits.readDouble((byte[]) o, (int) offset, BIG_ENDIAN);
    }

    @Override
    public double getDoubleVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putDouble(long address, double x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putDouble(Object o, long offset, double x) {
        validateObject(o);
        checkOffset(offset);
        DirectMemoryBits.writeDouble((byte[]) o, (int) offset, x, BIG_ENDIAN);
    }

    @Override
    public void putDoubleVolatile(Object o, long offset, double x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object getObject(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object getObjectVolatile(Object o, long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putObject(Object o, long offset, Object x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putObjectVolatile(Object o, long offset, Object x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean compareAndSwapInt(Object o, long offset, int expected, int x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean compareAndSwapLong(Object o, long offset, long expected, long x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public boolean compareAndSwapObject(Object o, long offset, Object expected, Object x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putOrderedInt(Object o, long offset, int x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putOrderedLong(Object o, long offset, long x) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void putOrderedObject(Object o, long offset, Object x) {
        throw new UnsupportedOperationException("Not supported");
    }
}
