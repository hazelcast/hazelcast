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
import com.hazelcast.jet.memory.JetMemoryException;
import com.hazelcast.jet.memory.JetOutOfMemoryException;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of MemoryBlockChain which is also a MemoryAllocator.
 */
public class DefaultMemoryBlockChain implements MemoryBlockChain, MemoryAllocator {
    protected final List<MemoryBlock> chain;
    protected MemoryBlock currentBlock;

    private final MemoryContext memoryContext;

    private MemoryType nextMemoryType;
    private MemoryType currentMemoryType;
    private int blockIndex;

    public DefaultMemoryBlockChain() {
        chain = new ArrayList<>();
        memoryContext = null;
    }

    public DefaultMemoryBlockChain(MemoryContext memoryContext, boolean enableReverseAllocator,
                                   MemoryChainingRule memoryChainingRule
    ) {
        chain = new ArrayList<>();
        this.memoryContext = memoryContext;
        this.nextMemoryType = memoryChainingRule.getNextMemoryType();
        this.currentMemoryType = memoryChainingRule.getCurrentMemoryType();
        if (!acquireNext(enableReverseAllocator)) {
            throw new JetMemoryException("Can't create chain without at least one segment. "
                    + "Probably segment size is too big or memory size too small");
        }
    }

    @Override
    public long allocate(long size) {
        return getAllocator().allocate(size);
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        return getAllocator().reallocate(address, currentSize, newSize);
    }

    @Override
    public boolean acquireNext(boolean enableReverseAllocator) {
        currentBlock = memoryContext.getMemoryBlockPool(currentMemoryType).getNextMemoryBlock(enableReverseAllocator);
        if (currentBlock == null) {
            if (nextMemoryType == null) {
                throw new JetOutOfMemoryException("Not enough memory of type " + this.currentMemoryType + "]");
            }
            this.currentMemoryType = nextMemoryType;
            this.nextMemoryType = null;
        } else {
            add(currentBlock);
            setCurrent(chain.size() - 1);
            return true;
        }
        currentBlock = memoryContext.getMemoryBlockPool(currentMemoryType).getNextMemoryBlock(enableReverseAllocator);
        if (currentBlock == null) {
            setCurrent(0);
            return false;
        } else {
            add(currentBlock);
            setCurrent(chain.size() - 1);
            return true;
        }
    }

    @Override
    public MemoryBlock current() {
        return currentBlock;
    }

    @Override
    public void setCurrent(int idx) {
        blockIndex = idx;
        currentBlock = chain.get(blockIndex);
    }

    @Override
    public MemoryBlock get(int blockIndex) {
        return chain.get(blockIndex);
    }

    @Override
    public void add(MemoryBlock element) {
        chain.add(element);
    }

    @Override
    public boolean gotoNext() {
        if (blockIndex >= chain.size() - 1) {
            return false;
        }
        blockIndex++;
        currentBlock = chain.get(blockIndex);
        return true;
    }

    @Override
    public void clear() {
        chain.clear();
    }

    @Override
    public int size() {
        return chain.size();
    }

    @Override
    public MemoryAllocator getAllocator() {
        return currentBlock.getAllocator();
    }

    @Override
    public MemoryAccessor getAccessor() {
        return currentBlock.getAccessor();
    }

    @Override
    public void dispose() {
        for (MemoryBlock memoryBlock : chain) {
            memoryBlock.reset();
            MemoryBlockPool memoryBlockPool = memoryContext.getMemoryBlockPool(memoryBlock.type());
            memoryBlockPool.releaseMemoryBlock(memoryBlock);
        }
    }

    @Override
    public void free(long address, long size) {
        getAllocator().free(address, size);
    }
}
