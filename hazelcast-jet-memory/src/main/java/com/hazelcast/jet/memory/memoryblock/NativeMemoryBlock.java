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

import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.jet.memory.util.AddressHolder;
import com.hazelcast.jet.memory.util.Util;

/**
 * A native memory block.
 */
public class NativeMemoryBlock extends BaseMemoryBlock {
    private final long baseAddress;
    private final AddressHolder addressHolder = new AddressHolder();

    public NativeMemoryBlock(long blockSize, boolean enableReverseAllocator, boolean useBigEndian) {
        super(blockSize, enableReverseAllocator, useBigEndian);
        baseAddress = UnsafeUtil.UNSAFE.allocateMemory(blockSize);
        this.addressHolder.setAddress(baseAddress);
    }

    @Override
    public long toAddress(long pos) {
        return baseAddress + pos;
    }

    @Override
    public void dispose() {
        UnsafeUtil.UNSAFE.freeMemory(baseAddress);
    }

    @Override
    public MemoryType type() {
        return MemoryType.NATIVE;
    }

    @Override
    protected void copyFromHeapBlock(
            HeapMemoryBlock sourceMemoryBlock, long sourceAddress, long dstAddress, long size
    ) {
        Util.assertPositiveInt(size);
        Util.assertPositiveInt(sourceAddress);
        getAccessor().copyFromByteArray(sourceMemoryBlock.getBackingArray(), (int) sourceAddress, dstAddress, (int) size);
    }

    @Override
    protected void copyFromNativeBlock(NativeMemoryBlock ignored, long sourceAddress, long dstAddress, long size) {
        getAccessor().copyMemory(sourceAddress, dstAddress, size);
    }

    @Override
    public MemoryAccessor getAccessor() {
        return GlobalMemoryAccessorRegistry.MEM;
    }
}
