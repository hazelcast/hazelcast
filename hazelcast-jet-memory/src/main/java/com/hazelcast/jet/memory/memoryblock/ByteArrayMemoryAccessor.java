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

package com.hazelcast.jet.memory.memoryblock;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.impl.EndiannessUtil;

import java.util.Arrays;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.BYTE_ARRAY_ACCESS;

/**
 * Accessor of {@code byte[]}-based memory.
 */
public class ByteArrayMemoryAccessor implements MemoryAccessor {

    private final boolean isBigEndian;
    private final byte[] memory;

    public ByteArrayMemoryAccessor(byte[] memory, boolean isBigEndian) {
        this.isBigEndian = isBigEndian;
        this.memory = memory;
    }

    @Override
    public boolean isBigEndian() {
        return isBigEndian;
    }

    @Override
    public boolean getBoolean(long address) {
        return memory[(int) address] > 0;
    }

    @Override
    public void putBoolean(long address, boolean x) {
        memory[(int) address] = x ? (byte) 1 : (byte) 0;
    }

    @Override
    public byte getByte(long address) {
        return memory[(int) address];
    }

    @Override
    public void putByte(long address, byte x) {
        memory[(int) address] = x;
    }

    @Override
    public char getChar(long address) {
        return EndiannessUtil.readChar(BYTE_ARRAY_ACCESS, memory, address, isBigEndian);
    }

    @Override
    public void putChar(long address, char x) {
        EndiannessUtil.writeChar(BYTE_ARRAY_ACCESS, memory, address, x, isBigEndian);
    }

    @Override
    public short getShort(long address) {
        return EndiannessUtil.readShort(BYTE_ARRAY_ACCESS, memory, address, isBigEndian);
    }

    @Override
    public void putShort(long address, short x) {
        EndiannessUtil.writeShort(BYTE_ARRAY_ACCESS, memory, address, x, isBigEndian);
    }

    @Override
    public int getInt(long address) {
        return EndiannessUtil.readInt(BYTE_ARRAY_ACCESS, memory, address, isBigEndian);
    }

    @Override
    public void putInt(long address, int x) {
        EndiannessUtil.writeInt(BYTE_ARRAY_ACCESS, memory, address, x, isBigEndian);
    }

    @Override
    public float getFloat(long address) {
        return EndiannessUtil.readFloat(BYTE_ARRAY_ACCESS, memory, address, isBigEndian);
    }

    @Override
    public void putFloat(long address, float x) {
        EndiannessUtil.writeFloat(BYTE_ARRAY_ACCESS, memory, address, x, isBigEndian);
    }

    @Override
    public long getLong(long address) {
        return EndiannessUtil.readLong(BYTE_ARRAY_ACCESS, memory, address, isBigEndian);
    }

    @Override
    public void putLong(long address, long x) {
        EndiannessUtil.writeLong(BYTE_ARRAY_ACCESS, memory, address, x, isBigEndian);
    }

    @Override
    public double getDouble(long address) {
        return EndiannessUtil.readDouble(BYTE_ARRAY_ACCESS, memory, address, isBigEndian);
    }

    @Override
    public void putDouble(long address, double x) {
        EndiannessUtil.writeDouble(BYTE_ARRAY_ACCESS, memory, address, x, isBigEndian);
    }

    @Override
    public void copyMemory(long srcAddress, long destAddress, long lengthBytes) {
        System.arraycopy(memory, (int) srcAddress, memory, (int) destAddress, (int) lengthBytes);
    }

    @Override
    public void copyFromByteArray(byte[] source, int offset, long destAddress, int length) {
        System.arraycopy(source, offset, memory, (int) destAddress, length);
    }

    @Override
    public void copyToByteArray(long srcAddress, byte[] destination, int offset, int length) {
        System.arraycopy(memory, (int) srcAddress, destination, offset, length);
    }

    @Override
    public void setMemory(long address, long lengthBytes, byte value) {
        Arrays.fill(memory, (int) address, (int) (address + lengthBytes), value);
    }
}
