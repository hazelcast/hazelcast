package com.hazelcast.monitor;

public interface LocalMemoryStats extends LocalInstanceStats {

    long getTotalPhysical();

    long getFreePhysical();

    long getMaxHeap();

    long getCommittedHeap();

    long getUsedHeap();

    long getFreeHeap();

    long getMaxOffHeap();

    long getCommittedOffHeap();

    long getUsedOffHeap();

    long getFreeOffHeap();

    LocalGCStats getGCStats();

}
