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

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;

/**
 * Provides little-endian access on a natively big-endian platform.
 */
public class LittleEndianMemoryAccessor extends EndianAccessorBase {

    @Override
    public boolean isBigEndian() {
        return false;
    }

    @Override
    public char getChar(Object base, long offset) {
        return EndiannessUtil.readCharL(EndiannessUtil.NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putChar(Object base, long offset, char x) {
        EndiannessUtil.writeCharL(EndiannessUtil.NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public short getShort(Object base, long offset) {
        return EndiannessUtil.readShortL(EndiannessUtil.NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putShort(Object base, long offset, short x) {
        EndiannessUtil.writeShortL(EndiannessUtil.NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public int getInt(Object base, long offset) {
        return EndiannessUtil.readIntL(EndiannessUtil.NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putInt(Object base, long offset, int x) {
        EndiannessUtil.writeIntL(EndiannessUtil.NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public float getFloat(Object base, long offset) {
        return EndiannessUtil.readFloatL(EndiannessUtil.NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putFloat(Object base, long offset, float x) {
        EndiannessUtil.writeFloatL(EndiannessUtil.NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public long getLong(Object base, long offset) {
        return EndiannessUtil.readLongL(EndiannessUtil.NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putLong(Object base, long offset, long x) {
        EndiannessUtil.writeLongL(EndiannessUtil.NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public double getDouble(Object base, long offset) {
        return EndiannessUtil.readDoubleL(EndiannessUtil.NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putDouble(Object base, long offset, double x) {
        EndiannessUtil.writeDoubleL(EndiannessUtil.NATIVE_ACCESS, base, offset, x);
    }



    @Override
    public char getChar(long address) {
        return EndiannessUtil.readCharL(EndiannessUtil.NATIVE_ACCESS, null, address);
    }

    @Override
    public void putChar(long address, char x) {
        EndiannessUtil.writeCharL(EndiannessUtil.NATIVE_ACCESS, null, address, x);
    }

    @Override
    public short getShort(long address) {
        return EndiannessUtil.readShortL(EndiannessUtil.NATIVE_ACCESS, null, address);
    }

    @Override
    public void putShort(long address, short x) {
        EndiannessUtil.writeShortL(EndiannessUtil.NATIVE_ACCESS, null, address, x);
    }

    @Override
    public int getInt(long address) {
        return EndiannessUtil.readIntL(EndiannessUtil.NATIVE_ACCESS, null, address);
    }

    @Override
    public void putInt(long address, int x) {
        EndiannessUtil.writeIntL(EndiannessUtil.NATIVE_ACCESS, null, address, x);
    }

    @Override
    public float getFloat(long address) {
        return EndiannessUtil.readFloatL(EndiannessUtil.NATIVE_ACCESS, null, address);
    }

    @Override
    public void putFloat(long address, float x) {
        EndiannessUtil.writeFloatL(EndiannessUtil.NATIVE_ACCESS, null, address, x);
    }

    @Override
    public long getLong(long address) {
        return EndiannessUtil.readLongL(EndiannessUtil.NATIVE_ACCESS, null, address);
    }

    @Override
    public void putLong(long address, long x) {
        EndiannessUtil.writeLongL(EndiannessUtil.NATIVE_ACCESS, null, address, x);
    }

    @Override
    public double getDouble(long address) {
        return EndiannessUtil.readDoubleL(EndiannessUtil.NATIVE_ACCESS, null, address);
    }

    @Override
    public void putDouble(long address, double x) {
        EndiannessUtil.writeDoubleL(EndiannessUtil.NATIVE_ACCESS, null, address, x);
    }

    @Override
    public void copyMemory(long srcAddress, long destAddress, long lengthBytes) {
        MEM.copyMemory(srcAddress, destAddress, lengthBytes);
    }

    @Override
    public void copyFromByteArray(byte[] source, int offset, long destAddress, int length) {
        MEM.copyMemory(source, ARRAY_BYTE_BASE_OFFSET + ARRAY_BYTE_INDEX_SCALE * offset, null, destAddress, length);
    }

    @Override
    public void copyToByteArray(long srcAddress, byte[] destination, int offset, int length) {
        MEM.copyMemory(null, srcAddress, destination, ARRAY_BYTE_BASE_OFFSET + ARRAY_BYTE_INDEX_SCALE * offset, length);
    }

    @Override
    public void setMemory(long address, long lengthBytes, byte value) {
        MEM.setMemory(address, lengthBytes, value);
    }
}
