/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.memory.MemoryAllocator;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * The implementation of internal index stats specialized for HD global indexes.
 * <p>
 * The main trait of the implementation is the concurrency support, which is
 * required for HD global indexes because they are shared among partitions.
 */
public final class HDGlobalPerIndexStats extends GlobalPerIndexStats {

    private static final AtomicLongFieldUpdater<HDGlobalPerIndexStats> MEMORY_COST = newUpdater(HDGlobalPerIndexStats.class,
            "memoryCost");

    private volatile long memoryCost;

    public HDGlobalPerIndexStats(boolean ordered, boolean usesCachedQueryableEntries) {
        super(ordered, usesCachedQueryableEntries);
    }

    @Override
    public long getMemoryCost() {
        return memoryCost;
    }

    @Override
    public MemoryAllocator wrapMemoryAllocator(MemoryAllocator memoryAllocator) {
        return new MemoryAllocatorWithStats(memoryAllocator);
    }

    private void updateMemoryCost(long delta) {
        MEMORY_COST.addAndGet(HDGlobalPerIndexStats.this, delta);
    }

    private void resetMemoryCost() {
        memoryCost = 0;
    }

    private class MemoryAllocatorWithStats implements MemoryAllocator {

        private final MemoryAllocator delegate;

        MemoryAllocatorWithStats(MemoryAllocator delegate) {
            this.delegate = delegate;
        }

        @Override
        public long allocate(long size) {
            long result = delegate.allocate(size);
            updateMemoryCost(size);
            return result;
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            long result = delegate.reallocate(address, currentSize, newSize);
            updateMemoryCost(newSize - currentSize);
            return result;
        }

        @Override
        public void free(long address, long size) {
            delegate.free(address, size);
            updateMemoryCost(-size);
        }

        @Override
        public void dispose() {
            delegate.dispose();
            resetMemoryCost();
        }
    }

    @Override
    public IndexOperationStats createOperationStats() {
        return new HDGlobalIndexOperationStats();
    }

}
