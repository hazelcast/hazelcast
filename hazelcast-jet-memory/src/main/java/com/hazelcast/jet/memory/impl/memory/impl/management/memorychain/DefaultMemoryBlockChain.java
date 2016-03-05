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

package com.hazelcast.jet.memory.impl.memory.impl.management.memorychain;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.jet.memory.spi.memory.MemoryType;
import com.hazelcast.jet.memory.spi.memory.MemoryContext;
import com.hazelcast.jet.memory.spi.memory.MemoryChainingType;
import com.hazelcast.jet.memory.api.memory.OutOfMemoryException;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlock;
import com.hazelcast.jet.memory.api.memory.management.MemoryBlockChain;
import com.hazelcast.jet.memory.impl.memory.impl.management.memoryblock.MemoryBlockPool;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DefaultMemoryBlockChain
        implements MemoryBlockChain, MemoryAllocator {
    private final MemoryContext memoryContext;

    private MemoryType nextMemoryType;
    private MemoryType currentMemoryType;

    private int blockIndex;
    protected final List<MemoryBlock> chain;
    protected MemoryBlock activeMemoryElement;

    public DefaultMemoryBlockChain() {
        chain = new ArrayList<MemoryBlock>();
        memoryContext = null;
    }

    public DefaultMemoryBlockChain(MemoryContext memoryContext,
                                   boolean useAux,
                                   MemoryChainingType memoryChainingType) {
        chain = new ArrayList<>();
        this.memoryContext = memoryContext;
        this.nextMemoryType = memoryChainingType.getNextMemoryType();
        this.currentMemoryType = memoryChainingType.getCurrentMemoryType();

        if (!acquireNextBlock(useAux)) {
            throw new IllegalStateException("Can't create chain without at least one segment. "
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
    public boolean acquireNextBlock(boolean useAux) {
        activeMemoryElement =
                this.memoryContext.getMemoryBlockPool(currentMemoryType).getNextMemoryBlock(useAux);

        if (activeMemoryElement == null) {
            if (this.nextMemoryType == null) {
                throw new OutOfMemoryException("Not enough memory of type :[" + this.currentMemoryType + "]");
            }

            this.currentMemoryType = this.nextMemoryType;
            this.nextMemoryType = null;
        } else {
            addElement(activeMemoryElement);
            gotoElement(chain.size() - 1);
            return true;
        }

        activeMemoryElement =
                this.memoryContext.getMemoryBlockPool(this.currentMemoryType).getNextMemoryBlock(useAux);

        if (activeMemoryElement == null) {
            gotoElement(0);
            return false;
        } else {
            addElement(activeMemoryElement);
            gotoElement(chain.size() - 1);
            return true;
        }
    }

    @Override
    public MemoryBlock activeElement() {
        return activeMemoryElement;
    }

    @Override
    public void gotoElement(int idx) {
        blockIndex = idx;
        activeMemoryElement = chain.get(blockIndex);
    }

    @Override
    public MemoryBlock getElement(int blockIndex) {
        return chain.get(blockIndex);
    }

    @Override
    public MemoryBlock remove(int blockIndex) {
        return chain.remove(blockIndex);
    }

    @Override
    public void addElement(MemoryBlock element) {
        chain.add(element);
    }

    @Override
    public boolean stepNoNext() {
        if (blockIndex < chain.size() - 1) {
            blockIndex++;
            activeMemoryElement = chain.get(blockIndex);
            return true;
        }

        return false;
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
        return activeMemoryElement.getAllocator();
    }

    @Override
    public MemoryAccessor getAccessor() {
        return activeMemoryElement.getAccessor();
    }

    @Override
    public void dispose() {
        for (MemoryBlock memoryBlock : chain) {
            memoryBlock.reset();
            MemoryBlockPool memoryBlockPool =
                    this.memoryContext.getMemoryBlockPool(memoryBlock.getMemoryType());

            memoryBlockPool.releaseMemoryBlock(memoryBlock);
        }
    }

    @Override
    public void free(long address, long size) {
        getAllocator().free(address, size);
    }
}
