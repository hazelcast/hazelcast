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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides api to reserve and monitor memory used by JET job
 */
public class MemoryPool {
    private final long total;
    private final MemoryType memoryType;
    private final AtomicLong usedMemory = new AtomicLong(0L);

    public MemoryPool(long totalSizeInBytes, MemoryType memoryType) {
        this.total = totalSizeInBytes;
        this.memoryType = memoryType;
    }

    public boolean reserve(long memorySize) {
        while (true) {
            long currentValue = usedMemory.get();
            long newValue = currentValue + memorySize;
            if (newValue > total) {
                return false;
            }
            if (usedMemory.compareAndSet(currentValue, newValue)) {
                return true;
            }
        }
    }

    public MemoryType getMemoryType() {
        return memoryType;
    }

    public void release(long memorySize) {
        usedMemory.addAndGet(-memorySize);
    }

    public long getFree() {
        return getTotal() - getUsed();
    }

    public long getUsed() {
        return usedMemory.get();
    }

    public long getTotal() {
        return total;
    }
}
