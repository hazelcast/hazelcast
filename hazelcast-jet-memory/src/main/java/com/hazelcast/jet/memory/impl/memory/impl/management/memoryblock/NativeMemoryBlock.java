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

import com.hazelcast.internal.memory.HeapMemoryAccessor;
import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.impl.util.AddressHolder;
import com.hazelcast.jet.memory.impl.util.Util;
import com.hazelcast.jet.memory.spi.memory.MemoryType;

import static com.hazelcast.internal.memory.impl.EndiannessUtil.NATIVE_ACCESS;

public class NativeMemoryBlock
        extends BaseMemoryBlock<AddressHolder> {
    private final long baseAddress;
    private final AddressHolder addressHolder = new AddressHolder();

    public NativeMemoryBlock(long blockSize,
                             boolean useAux,
                             boolean useBigEndian) {
        super(blockSize, useAux, useBigEndian);
        baseAddress = UnsafeUtil.UNSAFE.allocateMemory(blockSize);
        this.addressHolder.setAddress(baseAddress);
    }

    @Override
    public long toAddress(long pos) {
        return baseAddress + pos;
    }

    @Override
    public boolean getBoolean(long address) {
        return UnsafeUtil.UNSAFE.getBoolean(null, address);
    }

    @Override
    public void putBoolean(long address, boolean x) {
        UnsafeUtil.UNSAFE.putBoolean(null, address, x);
    }

    @Override
    public byte getByte(long address) {
        return UnsafeUtil.UNSAFE.getByte(address);
    }

    @Override
    public void putByte(long address, byte x) {
        UnsafeUtil.UNSAFE.putByte(address, x);
    }

    @Override
    public char getChar(long address) {
        return EndiannessUtil.readChar(NATIVE_ACCESS, null, address, useBigEndian);
    }

    @Override
    public void putChar(long address, char x) {
        EndiannessUtil.writeChar(NATIVE_ACCESS, null, address, x, useBigEndian);
    }

    @Override
    public short getShort(long address) {
        return EndiannessUtil.readShort(NATIVE_ACCESS, null, address, useBigEndian);
    }

    @Override
    public void putShort(long address, short x) {
        EndiannessUtil.writeShort(NATIVE_ACCESS, null, address, x, useBigEndian);
    }

    @Override
    public int getInt(long address) {
        return EndiannessUtil.readInt(NATIVE_ACCESS, null, address, useBigEndian);
    }

    @Override
    public void putInt(long address, int x) {
        EndiannessUtil.writeInt(NATIVE_ACCESS, null, address, x, useBigEndian);
    }

    @Override
    public float getFloat(long address) {
        return EndiannessUtil.readFloat(NATIVE_ACCESS, null, address, useBigEndian);
    }

    @Override
    public void putFloat(long address, float x) {
        EndiannessUtil.writeFloat(NATIVE_ACCESS, null, address, x, useBigEndian);
    }

    @Override
    public long getLong(long address) {
        return EndiannessUtil.readLong(NATIVE_ACCESS, null, address, useBigEndian);
    }

    @Override
    public void putLong(long address, long x) {
        EndiannessUtil.writeLong(NATIVE_ACCESS, null, address, x, useBigEndian);
    }

    @Override
    public double getDouble(long address) {
        return EndiannessUtil.readDouble(NATIVE_ACCESS, null, address, useBigEndian);
    }

    @Override
    public void putDouble(long address, double x) {
        EndiannessUtil.writeDouble(NATIVE_ACCESS, null, address, x, useBigEndian);
    }

    @Override
    public void copyMemory(long srcAddress, long destAddress, long lengthBytes) {
        UnsafeUtil.UNSAFE.copyMemory(srcAddress, destAddress, lengthBytes);
    }

    @Override
    public void copyFromByteArray(byte[] source, int offset, long destAddress, int length) {
        UnsafeUtil.UNSAFE.copyMemory(
                source,
                HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET + offset * HeapMemoryAccessor.ARRAY_BYTE_INDEX_SCALE,
                null,
                destAddress,
                (long) length
        );
    }

    @Override
    public void copyToByteArray(long srcAddress, byte[] destination, int offset, int length) {
        UnsafeUtil.UNSAFE.copyMemory(
                null,
                srcAddress,
                destination,
                HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET + offset * HeapMemoryAccessor.ARRAY_BYTE_INDEX_SCALE,
                (long) length
        );
    }

    @Override
    public void setMemory(long address, long lengthBytes, byte value) {
        UnsafeUtil.UNSAFE.setMemory(address, lengthBytes, value);
    }

    @Override
    public void dispose() {
        UnsafeUtil.UNSAFE.freeMemory(baseAddress);
    }

    @Override
    public MemoryType getMemoryType() {
        return MemoryType.NATIVE;
    }

    @Override
    public AddressHolder getResource() {
        return addressHolder;
    }

    @Override
    protected void copyFromHeapBlock(MemoryBlock<byte[]> sourceMemoryBlock,
                                     long sourceAddress,
                                     long dstAddress,
                                     long size) {
        Util.assertPositiveInt(size);
        Util.assertPositiveInt(sourceAddress);

        copyFromByteArray(
                sourceMemoryBlock.getResource(),
                (int) sourceAddress,
                dstAddress,
                (int) size
        );
    }

    @Override
    protected void copyFromNativeBlock(MemoryBlock<AddressHolder> sourceMemoryBlock,
                                       long sourceAddress,
                                       long dstAddress,
                                       long size) {
        copyMemory(
                sourceAddress,
                dstAddress,
                size
        );
    }
}
