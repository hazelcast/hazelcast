package com.hazelcast.dataset.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.Map;

public abstract class SegmentRun<R> {

    public final Unsafe unsafe = UnsafeUtil.UNSAFE;

    public long dataAddress;
    public long indicesAddress;
    public long recordDataSize;
    public long recordCount;
    public long indexOffset;
    public boolean indicesAvailable;

    public final void runAllFullScan(Segment segment) {
        while (segment != null) {
            dataAddress = segment.dataAddress();
            recordCount = segment.count();
            runFullScan();
            segment = segment.previous;
        }
    }

    protected abstract void runFullScan();

    public final void runAllWithIndex(Segment segment){
        while (segment != null) {
            dataAddress = segment.dataAddress();
            recordCount = segment.count();
            indicesAddress = segment.indicesAddress();
            runWithIndex();
            segment = segment.previous;
        }
    }

    protected abstract void runWithIndex();

    public abstract void bind(Map<String, Object> bindings);

    public abstract R result();
}
