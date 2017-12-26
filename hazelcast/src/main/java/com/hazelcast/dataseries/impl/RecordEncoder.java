package com.hazelcast.dataseries.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

/**
 * Responsible for reading and writing a record to offheap.
 *
 * todo: for every field there will probably be a method generated? e.g. putPrice and getPrice
 */
public abstract class RecordEncoder<R> {

    protected final Unsafe unsafe = UnsafeUtil.UNSAFE;
    protected final RecordModel recordModel;
    protected final long recordDataOffset;
    protected final int recordPayloadSize;

    protected RecordEncoder(RecordModel recordModel) {
        this.recordModel = recordModel;
        this.recordDataOffset = recordModel.getDataOffset();
        this.recordPayloadSize = recordModel.getPayloadSize();
    }

    public abstract R newInstance();

    public abstract void writeRecord(R record, long segmentAddress, int recordOffset, long indicesAddress);

    public abstract void readRecord(R record, long segmentAddress, int recordOffset);
}
