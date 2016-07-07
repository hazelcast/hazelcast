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
import com.hazelcast.jet.memory.util.Util;

/**
 * Memory block backed by a Java {@code byte[]}.
 */
public class HeapMemoryBlock extends BaseMemoryBlock {
    private byte[] data;
    private final ByteArrayMemoryAccessor accessor;

    public HeapMemoryBlock(long blockSize, boolean enableReverseAllocator, boolean useBigEndian) {
        super(blockSize, enableReverseAllocator, useBigEndian);
        this.data = new byte[(int) blockSize];
        this.accessor = new ByteArrayMemoryAccessor(data, useBigEndian);
    }

    byte[] getBackingArray() {
        return data;
    }

    @Override
    public void dispose() {
        data = null;
    }

    @Override
    public MemoryType type() {
        return MemoryType.HEAP;
    }

    @Override
    protected void copyFromHeapBlock(
            HeapMemoryBlock source, long sourceAddress, long dstAddress, long size
    ) {
        Util.assertPositiveInt(size);
        Util.assertPositiveInt(dstAddress);
        source.getAccessor().copyToByteArray(sourceAddress, data, (int) dstAddress, (int) size);
    }

    @Override
    protected void copyFromNativeBlock(
            NativeMemoryBlock source, long sourceAddress, long dstAddress, long size
    ) {
        Util.assertPositiveInt(size);
        Util.assertPositiveInt(dstAddress);
        source.getAccessor().copyToByteArray(sourceAddress, data, (int) dstAddress, (int) size);
    }

    @Override
    public long toAddress(long pos) {
        return pos;
    }

    @Override
    public MemoryAccessor getAccessor() {
        return accessor;
    }
}
