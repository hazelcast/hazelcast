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
    public void copyMemory(long srcAddress, long destAddress, long bytes) {
        UNSAFE.copyMemory(srcAddress, destAddress, bytes);
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

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putBoolean(long address, boolean x) {
        UNSAFE.putBoolean(null, address, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putByte(long address, byte x) {
        UNSAFE.putByte(address, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public char getChar(long address) {
        return UNSAFE.getChar(address);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putChar(long address, char x) {
        UNSAFE.putChar(address, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public short getShort(long address) {
        return UNSAFE.getShort(address);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putShort(long address, short x) {
        UNSAFE.putShort(address, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public int getInt(long address) {
        return UNSAFE.getInt(address);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putInt(long address, int x) {
        UNSAFE.putInt(address, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public float getFloat(long address) {
        return UNSAFE.getFloat(address);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putFloat(long address, float x) {
        UNSAFE.putFloat(address, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public long getLong(long address) {
        return UNSAFE.getLong(address);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putLong(long address, long x) {
        UNSAFE.putLong(address, x);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public double getDouble(long address) {
        return UNSAFE.getDouble(address);
    }

    /////////////////////////////////////////////////////////////////////////

    @Override
    public void putDouble(long address, double x) {
        UNSAFE.putDouble(address, x);
    }
}
