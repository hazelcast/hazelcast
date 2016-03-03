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
 * Provides little-endian access on a natively big-endian platform.
 */
public class LittleEndianMemoryAccessor extends EndianAccessorBase {

    @Override
    public boolean isBigEndian() {
        return false;
    }

    @Override
    public char getChar(Object base, long offset) {
        return EndiannessUtil.readCharL(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putChar(Object base, long offset, char x) {
        EndiannessUtil.writeCharL(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public short getShort(Object base, long offset) {
        return EndiannessUtil.readShortL(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putShort(Object base, long offset, short x) {
        EndiannessUtil.writeShortL(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public int getInt(Object base, long offset) {
        return EndiannessUtil.readIntL(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putInt(Object base, long offset, int x) {
        EndiannessUtil.writeIntL(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public float getFloat(Object base, long offset) {
        return EndiannessUtil.readFloatL(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putFloat(Object base, long offset, float x) {
        EndiannessUtil.writeFloatL(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public long getLong(Object base, long offset) {
        return EndiannessUtil.readLongL(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putLong(Object base, long offset, long x) {
        EndiannessUtil.writeLongL(NATIVE_ACCESS, base, offset, x);
    }

    @Override
    public double getDouble(Object base, long offset) {
        return EndiannessUtil.readDoubleL(NATIVE_ACCESS, base, offset);
    }

    @Override
    public void putDouble(Object base, long offset, double x) {
        EndiannessUtil.writeDoubleL(NATIVE_ACCESS, base, offset, x);
    }



    @Override
    public char getChar(long address) {
        return EndiannessUtil.readCharL(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putChar(long address, char x) {
        EndiannessUtil.writeCharL(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public short getShort(long address) {
        return EndiannessUtil.readShortL(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putShort(long address, short x) {
        EndiannessUtil.writeShortL(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public int getInt(long address) {
        return EndiannessUtil.readIntL(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putInt(long address, int x) {
        EndiannessUtil.writeIntL(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public float getFloat(long address) {
        return EndiannessUtil.readFloatL(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putFloat(long address, float x) {
        EndiannessUtil.writeFloatL(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public long getLong(long address) {
        return EndiannessUtil.readLongL(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putLong(long address, long x) {
        EndiannessUtil.writeLongL(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public double getDouble(long address) {
        return EndiannessUtil.readDoubleL(NATIVE_ACCESS, null, address);
    }

    @Override
    public void putDouble(long address, double x) {
        EndiannessUtil.writeDoubleL(NATIVE_ACCESS, null, address, x);
    }

    @Override
    public void copyMemory(long srcAddress, long destAddress, long lengthBytes) {
        MEM.copyMemory(srcAddress, destAddress, lengthBytes);
    }

    @Override
    public void setMemory(long address, long lengthBytes, byte value) {
        MEM.setMemory(address, lengthBytes, value);
    }
}
