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
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.jet.memory.JetMemoryException;
import com.hazelcast.jet.memory.JetOutOfMemoryException;

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
 * |                      MainArea                    v pos      |
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
 * MainArea is used to store data (0-byte is reserved as null value).
 * <p>
 * WatermarkArea is used to detect when MainArea is full.
 * <p>
 * WatermarkArea's area size is enough to create at least one storage's structure.
 * <p>
 * After we detect that offset is out of WatermarkArea - we make a decision that block is full.
 * <p>
 * AuxArea is used for temporary objects: for example we need to find key from the disk,
 * we copy key's bytes to the AuxArea and pass its address to the comparator.
 */
public abstract class BaseMemoryBlock implements MemoryBlock, MemoryAllocator {

    protected final boolean useBigEndian;
    protected final long totalSize;

    protected long pos = MemoryBlock.TOP_OFFSET;
    protected long lastAllocatedSize;
    protected long usedSize = MemoryBlock.TOP_OFFSET;

    private final MemoryAllocator reverseAllocator;
    private final MemoryManager auxMemMgr;
    private MemoryAllocator auxAllocator;
    private long auxPos;

    protected BaseMemoryBlock(long size, boolean enableReverseAllocator, boolean useBigEndian) {
        this.totalSize = size;
        this.useBigEndian = useBigEndian;
        this.reverseAllocator = new ReverseAllocator();
        this.auxMemMgr = new AuxMemoryManager();
        reset(enableReverseAllocator);
    }

    @Override
    public long allocate(long size) {
        long currentPos = pos;
        pos = allocateMain(size, pos, auxPos);
        return toAddress(currentPos);
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        if (toAddress(pos) - currentSize != address) {
            throw new JetMemoryException(
                    "Attempt to reallocate a block which is not the last one allocated from this allocator");
        }
        if (newSize <= currentSize) {
            throw new JetMemoryException("Attempt to reallocate to a smaller size");
        }
        long size = newSize - currentSize;
        pos = allocateMain(size, pos, auxPos);
        lastAllocatedSize = (int) newSize;
        return address;
    }

    /**
     * When {@code address} is {@value MemoryAllocator#NULL_ADDRESS}, the semantics change: there will be no
     * "block at address zero" freed, instead the last allocated block will shrink by the given {@code size}.
     */
    @Override
    public void free(long address, long size) {
        if (address != NULL_ADDRESS && (pos - size != address || lastAllocatedSize != size)) {
            return;
        }
        usedSize -= size;
        pos -= size;
        lastAllocatedSize -= size;
    }

    @Override
    public void reset(boolean enableReverseAllocator) {
        auxAllocator = enableReverseAllocator ? reverseAllocator : this;
        auxPos = totalSize - 1;
        reset();
    }

    @Override
    public void reset() {
        reverseAllocator.dispose();
        usedSize = MemoryBlock.TOP_OFFSET;
        pos = MemoryBlock.TOP_OFFSET;
        lastAllocatedSize = 1L;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void copyFrom(MemoryBlock source, long sourceOffset, long dstOffset, long size) {
        switch (source.type()) {
            case HEAP:
                copyFromHeapBlock((HeapMemoryBlock) source, sourceOffset, dstOffset, size);
                return;
            case NATIVE:
                copyFromNativeBlock((NativeMemoryBlock) source, sourceOffset, dstOffset, size);
                return;
            default:
                throw new JetMemoryException("unhandled source.type()");
        }
    }

    @Override
    public MemoryManager getAuxMemoryManager() {
        return auxMemMgr;
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


    protected abstract void copyFromHeapBlock(HeapMemoryBlock source, long sourceAddress, long dstAddress, long size);

    protected abstract void copyFromNativeBlock(NativeMemoryBlock source, long sourceAddress, long dstAddress, long size);


    private long allocateMain(long size, long pos, long limit) {
        long newPos = checkAndAddNewPos(size, pos, limit);
        lastAllocatedSize = size;
        usedSize += size;
        return newPos;
    }

    private long allocateAux(long size, long pos, long limit) {
        long newPos = checkAndSubNewPos(size, pos, limit);
        usedSize += size;
        return newPos;
    }

    private long checkAndAddNewPos(long size, long pos, long limit) {
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
        return new JetOutOfMemoryException("block is full pos=" + pos);
    }

    private final class ReverseAllocator implements MemoryAllocator {
        @Override
        public long allocate(long size) {
            auxPos = allocateAux(size, auxPos, pos);
            return toAddress(auxPos + 1);
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            throw new UnsupportedOperationException("Unsupported for aux allocator");
        }

        @Override
        public void free(long address, long size) {
            if (address == NULL_ADDRESS) {
                auxPos += size;
                usedSize -= size;
            }
        }

        @Override
        public void dispose() {

        }
    }

    private final class AuxMemoryManager implements MemoryManager {
        @Override
        public MemoryAllocator getAllocator() {
            return auxAllocator;
        }

        @Override
        public MemoryAccessor getAccessor() {
            return BaseMemoryBlock.this.getAccessor();
        }

        @Override
        public void dispose() {
            BaseMemoryBlock.this.dispose();
        }
    }
}
