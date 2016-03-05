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

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;
import com.hazelcast.jet.memory.spi.memory.MemoryType;
import com.hazelcast.jet.memory.impl.util.AddressHolder;
import com.hazelcast.jet.memory.api.memory.OutOfMemoryException;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;

/**
 * The structure of the main memory block is:
 * <p>
 * <p>
 * <pre>
 * ---------------------------------------------------------------
 * |0-byte (null)|        Header with partition's data           |
 * ---------------------------------------------------------------
 * |                                                  |          |
 * |                                                  |          |
 * |                      DataArea                    v pos      |
 * ---------------------------------------------------------------
 * |                                                  ^ auxPos   |
 * |                      AuxArea  (If required)      |          |
 * |                                                  |          |
 * ---------------------------------------------------------------
 * </pre>
 * <p>
 * <p>
 * Each Memory block has a header:
 * <pre>
 * -----------------
 * | storageAddress|
 * -----------------
 * </pre>
 * <p>
 * The base structure of JET memory block is shown on above picture;
 * <p>
 * <p>
 * <p>
 * DataArea is used to store data (0-byte is reserved as null value);
 * <p>
 * WatermarkArea is used to detect when DataArea is full;
 * <p>
 * WatermarkArea's area size is enough to create at least one storage's structure;
 * <p>
 * After we detect that offset is out of WatermarkArea - we make a decision that block is full;
 * <p>
 * ServiceArea is used for temporary objects;
 * <p>
 * For example we need to find key from the disk; We copy key's bytes to the ServiceArea and
 * pass it's address to the comparator;
 */
public abstract class BaseMemoryBlock<Resource>
        implements MemoryBlock<Resource> {
    protected long pos = MemoryBlock.TOP_OFFSET;

    protected final long totalSize;

    protected long lastAllocatedSize;

    protected long lastAuxAllocatedSize;

    protected long usedSize = MemoryBlock.TOP_OFFSET;

    private long auxPos;

    private MemoryManager auxMemoryManager;

    protected final boolean useBigEndian;

    private final MemoryAllocator baseAuxProxy;

    private final MemoryAllocator baseAuxAllocator;

    private MemoryAllocator auxAllocator;

    public BaseMemoryBlock(long size,
                           boolean useAux,
                           boolean useBigEndian) {
        this.totalSize = size;
        this.useBigEndian = useBigEndian;
        this.baseAuxAllocator = new DefaultAuxAllocator();
        this.baseAuxProxy = new DefaultAuxProxyAllocator();
        this.auxMemoryManager = new DefaultAuxMemoryManager();
        reset(useAux);
    }


    @Override
    public long allocate(long size) {
        long currentPos = pos;
        pos = allocateFrom(size, pos, auxPos);
        return toAddress(currentPos);
    }

    private long allocateFrom(long size, long pos, long limit) {
        long newPos = checkAndAddNewPos(size, pos, limit);
        lastAllocatedSize = size;
        usedSize += size;
        return newPos;
    }

    private long allocateTo(long size, long pos, long limit) {
        long newPos = checkAndSubNewPos(size, pos, limit);
        lastAuxAllocatedSize = size;
        usedSize += size;
        return newPos;
    }

    @Override
    public long reallocate(long address,
                           long currentSize,
                           long newSize) {
        if (toAddress(pos) - currentSize != address) {
            throw new IllegalStateException(
                    "Can't re-allocate not last allocated memory currentSize=" + currentSize
                            + " newSize=" + newSize
                            + " delta=" + (pos - address)
            );
        }

        if (newSize <= currentSize) {
            throw new IllegalStateException(
                    "Can't acquire size which is less or equal to the current size"
            );
        }

        long size = newSize - currentSize;
        pos = allocateFrom(size, pos, auxPos);
        lastAllocatedSize = (int) newSize;
        return address;
    }

    private long checkAndAddNewPos(long size,
                                   long pos,
                                   long limit) {
        long newPos = pos + size;

        if (newPos >= limit) {
            throw raiseBlockIsFull();
        } else {
            return newPos;
        }
    }

    private long checkAndSubNewPos(long size, long pos, long limit) {
        long newPos = pos - size;

        if (newPos <= limit) {
            throw raiseBlockIsFull();
        } else {
            return newPos;
        }
    }

    private RuntimeException raiseBlockIsFull() {
        return new OutOfMemoryException("block is full pos=" + pos);
    }

    @Override
    public void free(long address, long size) {
        if ((address != MemoryUtil.NULL_VALUE) &&
                (
                        (pos - size != address)
                                ||
                                (lastAllocatedSize != size)
                )) {
            return;
        }

        usedSize -= size;
        pos -= size;
        lastAllocatedSize -= size;
    }

    @Override
    public void reset(boolean useAux) {
        auxAllocator = useAux
                ?
                baseAuxAllocator :
                this;

        auxPos = totalSize - 1;
        reset();
    }

    @Override
    public void reset() {
        baseAuxAllocator.dispose();
        usedSize = MemoryBlock.TOP_OFFSET;
        pos = MemoryBlock.TOP_OFFSET;
        lastAllocatedSize = 1L;
        lastAuxAllocatedSize = 0L;
    }

    protected abstract void copyFromHeapBlock(MemoryBlock<byte[]> sourceMemoryBlock,
                                              long sourceAddress,
                                              long dstAddress,
                                              long size);

    protected abstract void copyFromNativeBlock(MemoryBlock<AddressHolder> sourceMemoryBlock,
                                                long sourceAddress,
                                                long dstAddress,
                                                long size);

    @Override
    @SuppressWarnings("unchecked")
    public void copyFromMemoryBlock(MemoryBlock sourceMemoryBlock,
                                    long sourceOffset,
                                    long dstOffset,
                                    long size) {
        if (sourceMemoryBlock.getMemoryType() == MemoryType.HEAP) {
            copyFromHeapBlock(sourceMemoryBlock, sourceOffset, dstOffset, size);
        } else if (sourceMemoryBlock.getMemoryType() == MemoryType.NATIVE) {
            copyFromNativeBlock(sourceMemoryBlock, sourceOffset, dstOffset, size);
        }
    }

    public MemoryManager getAuxMemoryManager() {
        return auxMemoryManager;
    }

    @Override
    public long getUsedBytes() {
        return usedSize;
    }

    @Override
    public long getAvailableBytes() {
        return totalSize - usedSize;
    }

    @Override
    public long getTotalBytes() {
        return totalSize;
    }

    @Override
    public MemoryAllocator getAllocator() {
        return this;
    }

    @Override
    public MemoryAccessor getAccessor() {
        return this;
    }

    @Override
    public boolean isBigEndian() {
        return useBigEndian;
    }

    private final class DefaultAuxAllocator implements MemoryAllocator {
        @Override
        public long allocate(long size) {
            auxPos = allocateTo(size, auxPos, pos);
            return toAddress(auxPos + 1);
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            throw new UnsupportedOperationException("Unsupported for aux allocator");
        }

        @Override
        public void free(long address, long size) {
            if (address == MemoryUtil.NULL_VALUE) {
                auxPos += size;
                usedSize -= size;
            }
        }

        @Override
        public void dispose() {

        }
    }

    private final class DefaultAuxMemoryManager implements MemoryManager {
        @Override
        public MemoryAllocator getAllocator() {
            return baseAuxProxy;
        }

        @Override
        public MemoryAccessor getAccessor() {
            return BaseMemoryBlock.this;
        }

        @Override
        public void dispose() {
            BaseMemoryBlock.this.dispose();
        }
    }

    private class DefaultAuxProxyAllocator implements MemoryAllocator {
        @Override
        public long allocate(long size) {
            return auxAllocator.allocate(size);
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            return auxAllocator.reallocate(address, currentSize, newSize);
        }

        @Override
        public void free(long address, long size) {
            auxAllocator.free(address, size);
        }

        @Override
        public void dispose() {
            auxAllocator.dispose();
        }
    }
}
