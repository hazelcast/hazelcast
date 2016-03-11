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
import static com.hazelcast.internal.memory.impl.EndiannessUtil.NATIVE_ACCESS;

/**
 * Provides big-endian access on a natively little-endian platform.
 */
public class BigEndianMemoryAccessor extends EndianAccessorBase {

    @Override
    public boolean isBigEndian() {
        return true;
    }

    @Override
    public char getChar(Object base, long offset) {
        return EndiannessUtil.readCharB(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putChar(Object base, long offset, char x) {
        EndiannessUtil.writeCharB(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public short getShort(Object base, long offset) {
        return EndiannessUtil.readShortB(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putShort(Object base, long offset, short x) {
        EndiannessUtil.writeShortB(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public int getInt(Object base, long offset) {
        return EndiannessUtil.readIntB(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putInt(Object base, long offset, int x) {
        EndiannessUtil.writeIntB(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public float getFloat(Object base, long offset) {
        return EndiannessUtil.readFloatB(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putFloat(Object base, long offset, float x) {
        EndiannessUtil.writeFloatB(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public long getLong(Object base, long offset) {
        return EndiannessUtil.readLongB(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putLong(Object base, long offset, long x) {
        EndiannessUtil.writeLongB(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public double getDouble(Object base, long offset) {
        return EndiannessUtil.readDoubleB(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putDouble(Object base, long offset, double x) {
        EndiannessUtil.writeDoubleB(NATIVE_ACCESS, base, offset, x);
    }



    @Override
    public char getChar(long address) {
        return EndiannessUtil.readCharB(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putChar(long address, char x) {
        EndiannessUtil.writeCharB(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public short getShort(long address) {
        return EndiannessUtil.readShortB(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putShort(long address, short x) {
        EndiannessUtil.writeShortB(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public int getInt(long address) {
        return EndiannessUtil.readIntB(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putInt(long address, int x) {
        EndiannessUtil.writeIntB(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public float getFloat(long address) {
        return EndiannessUtil.readFloatB(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putFloat(long address, float x) {
        EndiannessUtil.writeFloatB(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public long getLong(long address) {
        return EndiannessUtil.readLongB(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putLong(long address, long x) {
        EndiannessUtil.writeLongB(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public double getDouble(long address) {
        return EndiannessUtil.readDoubleB(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putDouble(long address, double x) {
        EndiannessUtil.writeDoubleB(NATIVE_ACCESS, null, address, x);
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
