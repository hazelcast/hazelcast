package com.hazelcast.jet.memory.api.binarystorage.sorted;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.util.sort.QuickSorter;

public abstract class JetQuickSorter extends QuickSorter {
    protected MemoryAccessor memoryAccessor;

    public void setMemoryAccessor(MemoryAccessor memoryAccessor) {
        this.memoryAccessor = memoryAccessor;
    }
}
