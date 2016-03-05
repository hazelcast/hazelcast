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

package com.hazelcast.jet.memory.impl.memory.impl.management.memoryblock;

import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.impl.util.AddressHolder;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.spi.memory.MemoryType;

import java.util.Arrays;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.BYTE_ARRAY_ACCESS;

public class HeapMemoryBlock
        extends BaseMemoryBlock<byte[]> {
    private byte[] data;

    public HeapMemoryBlock(long blockSize,
                           boolean useAux,
                           boolean useBigEndian) {
        super(blockSize, useAux, useBigEndian);
        this.data = new byte[(int) blockSize];
    }

    @Override
    public boolean getBoolean(long address) {
        return data[(int) address] > 0;
    }

    @Override
    public void putBoolean(long address, boolean x) {
        data[(int) address] = x ? (byte) 1 : (byte) 0;
    }

    @Override
    public byte getByte(long address) {
        return data[(int) address];
    }

    @Override
    public void putByte(long address, byte x) {
        data[(int) address] = x;
    }

    @Override
    public char getChar(long address) {
        return EndiannessUtil.readChar(BYTE_ARRAY_ACCESS, data, address, useBigEndian);
    }

    @Override
    public void putChar(long address, char x) {
        EndiannessUtil.writeChar(BYTE_ARRAY_ACCESS, data, address, x, useBigEndian);
    }

    @Override
    public short getShort(long address) {
        return EndiannessUtil.readShort(BYTE_ARRAY_ACCESS, data, address, useBigEndian);
    }

    @Override
    public void putShort(long address, short x) {
        EndiannessUtil.writeShort(BYTE_ARRAY_ACCESS, data, address, x, useBigEndian);
    }

    @Override
    public int getInt(long address) {
        return EndiannessUtil.readInt(BYTE_ARRAY_ACCESS, data, address, useBigEndian);
    }

    @Override
    public void putInt(long address, int x) {
        EndiannessUtil.writeInt(BYTE_ARRAY_ACCESS, data, address, x, useBigEndian);
    }

    @Override
    public float getFloat(long address) {
        return EndiannessUtil.readFloat(BYTE_ARRAY_ACCESS, data, address, useBigEndian);
    }

    @Override
    public void putFloat(long address, float x) {
        EndiannessUtil.writeFloat(BYTE_ARRAY_ACCESS, data, address, x, useBigEndian);
    }

    @Override
    public long getLong(long address) {
        return EndiannessUtil.readLong(BYTE_ARRAY_ACCESS, data, address, useBigEndian);
    }

    @Override
    public void putLong(long address, long x) {
        EndiannessUtil.writeLong(BYTE_ARRAY_ACCESS, data, address, x, useBigEndian);
    }

    @Override
    public double getDouble(long address) {
        return EndiannessUtil.readDouble(BYTE_ARRAY_ACCESS, data, address, useBigEndian);
    }

    @Override
    public void putDouble(long address, double x) {
        EndiannessUtil.writeDouble(BYTE_ARRAY_ACCESS, data, address, x, useBigEndian);
    }

    @Override
    public void copyMemory(long srcAddress, long destAddress, long lengthBytes) {
        System.arraycopy(data, (int) srcAddress, data, (int) destAddress, (int) lengthBytes);
    }

    @Override
    public void copyFromByteArray(byte[] source, int offset, long destAddress, int length) {
        System.arraycopy(source, offset, data, (int) destAddress, length);
    }

    @Override
    public void copyToByteArray(long srcAddress, byte[] destination, int offset, int length) {
        System.arraycopy(data, (int) srcAddress, destination, offset, length);
    }

    @Override
    public void setMemory(long address, long lengthBytes, byte value) {
        Arrays.fill(data, (int) address, (int) (address + lengthBytes), value);
    }

    @Override
    public void dispose() {
        data = null;
    }

    @Override
    public MemoryType getMemoryType() {
        return MemoryType.HEAP;
    }

    @Override
    public byte[] getResource() {
        return data;
    }

    @Override
    protected void copyFromHeapBlock(MemoryBlock<byte[]> sourceMemoryBlock,
                                     long sourceAddress,
                                     long dstAddress,
                                     long size) {
        Util.assertPositiveInt(size);
        Util.assertPositiveInt(dstAddress);
        sourceMemoryBlock.copyToByteArray(sourceAddress, data, (int) dstAddress, (int) size);
    }

    @Override
    protected void copyFromNativeBlock(MemoryBlock<AddressHolder> sourceMemoryBlock,
                                       long sourceAddress,
                                       long dstAddress,
                                       long size) {
        Util.assertPositiveInt(size);
        Util.assertPositiveInt(dstAddress);
        sourceMemoryBlock.copyToByteArray(sourceAddress, data, (int) dstAddress, (int) size);
    }

    @Override
    public long toAddress(long pos) {
        return pos;
    }
}
