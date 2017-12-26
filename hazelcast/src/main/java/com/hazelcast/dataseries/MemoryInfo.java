package com.hazelcast.dataseries;

import java.io.Serializable;

public class MemoryInfo implements Serializable {
    private final long consumedBytes;
    private final long allocatedBytes;
    private final int segmentsInUse;
    private final long count;

    public MemoryInfo(long consumedBytes, long allocatedBytes, int segmentsInUse, long count) {
        this.consumedBytes = consumedBytes;
        this.allocatedBytes = allocatedBytes;
        this.segmentsInUse = segmentsInUse;
        this.count=count;
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

    public long count() {
        return count;
    }

    @Override
    public String toString() {
        return "MemoryInfo{" +
                "count=" + count +
                ", consumedBytes=" + consumedBytes +
                ", allocatedBytes=" + allocatedBytes +
                ", storageEfficiency=" + ((100d * consumedBytes) / allocatedBytes) + "%" +
                ", segmentsInUse=" + segmentsInUse +
                '}';
    }
}
