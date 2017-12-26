package com.hazelcast.internal.commitlog;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class UsageInfo implements DataSerializable {
    private long bytesConsumed;
    private long bytesAllocated;
    private long regionsInUse;
    private long count;

    private UsageInfo() {
    }

    public UsageInfo(long bytesConsumed, long bytesAllocated, long regionsInUse, long count) {
        this.bytesConsumed = bytesConsumed;
        this.bytesAllocated = bytesAllocated;
        this.regionsInUse = regionsInUse;
        this.count = count;
    }

    public long regionsInUse() {
        return regionsInUse;
    }

    public long bytesConsumed() {
        return bytesConsumed;
    }

    public long bytesAllocated() {
        return bytesAllocated;
    }

    public long count() {
        return count;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(bytesConsumed);
        out.writeLong(bytesAllocated);
        out.writeLong(regionsInUse);
        out.writeLong(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.bytesConsumed = in.readLong();
        this.bytesAllocated = in.readLong();
        this.regionsInUse = in.readLong();
        this.count = in.readLong();
    }

    @Override
    public String toString() {
        return "UsageInfo{"
                + "count=" + count
                + ", bytesConsumed=" + bytesConsumed
                + ", bytesConsumedPerEntry=" + (bytesConsumed / count)
                + ", bytesAllocated=" + bytesAllocated
                + ", storageEfficiency=" + ((100d * bytesConsumed) / bytesAllocated) + "%"
                + ", regionsInUse=" + regionsInUse
                + '}';
    }
}