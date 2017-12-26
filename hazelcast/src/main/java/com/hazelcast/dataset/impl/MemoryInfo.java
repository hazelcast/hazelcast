package com.hazelcast.dataset.impl;

import java.io.Serializable;

public class MemoryInfo implements Serializable {
    private final long consumedBytes;
    private final long allocatedBytes;
    private final int segmentsInUse;

    public MemoryInfo(long consumedBytes, long allocatedBytes, int segmentsInUse) {
        this.consumedBytes = consumedBytes;
        this.allocatedBytes = allocatedBytes;
        this.segmentsInUse = segmentsInUse;
    }

    public int segmentsInUse() {
        return segmentsInUse;
    }

    public long consumedBytes() {
        return consumedBytes;
    }

    public long allocatedBytes() {
        return allocatedBytes;
    }

    @Override
    public String toString() {
        return "MemoryInfo{" +
                "consumedBytes=" + consumedBytes +
                ", allocatedBytes=" + allocatedBytes +
                ", allocationEffectiveness=" + ((100d * consumedBytes) / allocatedBytes) + "%" +
                ", segmentsInUse=" + segmentsInUse +
                '}';
    }
}
