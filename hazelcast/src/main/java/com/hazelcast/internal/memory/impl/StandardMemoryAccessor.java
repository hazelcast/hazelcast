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

import java.lang.reflect.Field;

/**
 * Standard {@link com.hazelcast.internal.memory.MemoryAccessor} implementations
 * that directly uses {@link sun.misc.Unsafe} for accessing to memory.
 */
public class StandardMemoryAccessor extends UnsafeBasedMemoryAccessor {

    public StandardMemoryAccessor() {
        if (!AVAILABLE) {
            throw new IllegalStateException(getClass().getName() + " can only be used only when Unsafe is available!");
        }
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    @Override
    public int arrayBaseOffset(Class<?> arrayClass) {
        return UNSAFE.arrayBaseOffset(arrayClass);
    }

    @Override
    public int arrayIndexScale(Class<?> arrayClass) {
        return UNSAFE.arrayIndexScale(arrayClass);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void copyMemory(long srcAddress, long destAddress, long bytes) {
        UNSAFE.copyMemory(srcAddress, destAddress, bytes);
    }

    @Override
    public void copyMemory(Object srcObj, long srcOffset, Object destObj, long destOffset, long bytes) {
        UNSAFE.copyMemory(srcObj, srcOffset, destObj, destOffset, bytes);
    }

    @Override
    public void setMemory(long address, long bytes, byte value) {
        UNSAFE.setMemory(address, bytes, value);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public boolean getBoolean(long address) {
        return UNSAFE.getBoolean(null, address);
    }

    @Override
    public boolean getBoolean(Object o, long offset) {
        return UNSAFE.getBoolean(o, offset);
    }

    @Override
    public boolean getBooleanVolatile(Object o, long offset) {
        return UNSAFE.getBooleanVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putBoolean(long address, boolean x) {
        UNSAFE.putBoolean(null, address, x);
    }

    @Override
    public void putBoolean(Object o, long offset, boolean x) {
        UNSAFE.putBoolean(o, offset, x);
    }

    @Override
    public void putBooleanVolatile(Object o, long offset, boolean x) {
        UNSAFE.putBooleanVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    @Override
    public byte getByte(Object o, long offset) {
        return UNSAFE.getByte(o, offset);
    }

    @Override
    public byte getByteVolatile(Object o, long offset) {
        return UNSAFE.getByteVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putByte(long address, byte x) {
        UNSAFE.putByte(address, x);
    }

    @Override
    public void putByte(Object o, long offset, byte x) {
        UNSAFE.putByte(o, offset, x);
    }

    @Override
    public void putByteVolatile(Object o, long offset, byte x) {
        UNSAFE.putByteVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public char getChar(long address) {
        return UNSAFE.getChar(address);
    }

    @Override
    public char getChar(Object o, long offset) {
        return UNSAFE.getChar(o, offset);
    }

    @Override
    public char getCharVolatile(Object o, long offset) {
        return UNSAFE.getCharVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putChar(long address, char x) {
        UNSAFE.putChar(address, x);
    }

    @Override
    public void putChar(Object o, long offset, char x) {
        UNSAFE.putChar(o, offset, x);
    }

    @Override
    public void putCharVolatile(Object o, long offset, char x) {
        UNSAFE.putCharVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public short getShort(long address) {
        return UNSAFE.getShort(address);
    }

    @Override
    public short getShort(Object o, long offset) {
        return UNSAFE.getShort(o, offset);
    }

    @Override
    public short getShortVolatile(Object o, long offset) {
        return UNSAFE.getShortVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putShort(long address, short x) {
        UNSAFE.putShort(address, x);
    }

    @Override
    public void putShort(Object o, long offset, short x) {
        UNSAFE.putShort(o, offset, x);
    }

    @Override
    public void putShortVolatile(Object o, long offset, short x) {
        UNSAFE.putShortVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public int getInt(long address) {
        return UNSAFE.getInt(address);
    }

    @Override
    public int getInt(Object o, long offset) {
        return UNSAFE.getInt(o, offset);
    }

    @Override
    public int getIntVolatile(Object o, long offset) {
        return UNSAFE.getIntVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putInt(long address, int x) {
        UNSAFE.putInt(address, x);
    }

    @Override
    public void putInt(Object o, long offset, int x) {
        UNSAFE.putInt(o, offset, x);
    }

    @Override
    public void putIntVolatile(Object o, long offset, int x) {
        UNSAFE.putIntVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public float getFloat(long address) {
        return UNSAFE.getFloat(address);
    }

    @Override
    public float getFloat(Object o, long offset) {
        return UNSAFE.getFloat(o, offset);
    }

    @Override
    public float getFloatVolatile(Object o, long offset) {
        return UNSAFE.getFloatVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putFloat(long address, float x) {
        UNSAFE.putFloat(address, x);
    }

    @Override
    public void putFloat(Object o, long offset, float x) {
        UNSAFE.putFloat(o, offset, x);
    }

    @Override
    public void putFloatVolatile(Object o, long offset, float x) {
        UNSAFE.putFloatVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    @Override
    public long getLong(Object o, long offset) {
        return UNSAFE.getLong(o, offset);
    }

    @Override
    public long getLongVolatile(Object o, long offset) {
        return UNSAFE.getLongVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putLong(long address, long x) {
        UNSAFE.putLong(address, x);
    }

    @Override
    public void putLong(Object o, long offset, long x) {
        UNSAFE.putLong(o, offset, x);
    }

    @Override
    public void putLongVolatile(Object o, long offset, long x) {
        UNSAFE.putLongVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public double getDouble(long address) {
        return UNSAFE.getDouble(address);
    }

    @Override
    public double getDouble(Object o, long offset) {
        return UNSAFE.getDouble(o, offset);
    }

    @Override
    public double getDoubleVolatile(Object o, long offset) {
        return UNSAFE.getDoubleVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putDouble(long address, double x) {
        UNSAFE.putDouble(address, x);
    }

    @Override
    public void putDouble(Object o, long offset, double x) {
        UNSAFE.putDouble(o, offset, x);
    }

    @Override
    public void putDoubleVolatile(Object o, long offset, double x) {
        UNSAFE.putDoubleVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public Object getObject(Object o, long offset) {
        return UNSAFE.getObject(o, offset);
    }

    @Override
    public Object getObjectVolatile(Object o, long offset) {
        return UNSAFE.getObjectVolatile(o, offset);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putObject(Object o, long offset, Object x) {
        UNSAFE.putObject(o, offset, x);
    }

    @Override
    public void putObjectVolatile(Object o, long offset, Object x) {
        UNSAFE.putObjectVolatile(o, offset, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public boolean compareAndSwapInt(Object o, long offset, int expected, int x) {
        return UNSAFE.compareAndSwapInt(o, offset, expected, x);
    }

    @Override
    public boolean compareAndSwapLong(Object o, long offset, long expected, long x) {
        return UNSAFE.compareAndSwapLong(o, offset, expected, x);
    }

    @Override
    public boolean compareAndSwapObject(Object o, long offset, Object expected, Object x) {
        return UNSAFE.compareAndSwapObject(o, offset, expected, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putOrderedInt(Object o, long offset, int x) {
        UNSAFE.putOrderedInt(o, offset, x);
    }

    @Override
    public void putOrderedLong(Object o, long offset, long x) {
        UNSAFE.putOrderedLong(o, offset, x);
    }

    @Override
    public void putOrderedObject(Object o, long offset, Object x) {
        UNSAFE.putOrderedObject(o, offset, x);
    }

}
