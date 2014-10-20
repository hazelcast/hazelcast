package com.hazelcast.monitor;

public interface LocalMemoryStats extends LocalInstanceStats {

    long getTotalPhysical();

    long getFreePhysical();

    long getMaxHeap();

    long getCommittedHeap();

    long getUsedHeap();

    long getFreeHeap();

    long getMaxNativeMemory();

    long getCommittedNativeMemory();

    long getUsedNativeMemory();

    long getFreeNativeMemory();

    LocalGCStats getGCStats();

}
