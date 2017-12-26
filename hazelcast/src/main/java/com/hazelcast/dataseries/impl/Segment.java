package com.hazelcast.dataseries.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import sun.misc.Unsafe;

import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.lang.String.format;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

public class Segment {

    private final static AtomicIntegerFieldUpdater OWNERSHIP_COUNT = newUpdater(Segment.class, "ownershipCount");

    public volatile Segment next;
    public volatile Segment previous;

    // Provides 'smart pointer' like behavior so the segment can be shared between threads once it is tenured.
    // A segment is 'immutable' from that point on, but its resources need to be released once nobody is referring
    // to the segment. Using this mechanism we can safely share segments between threads.
    // we start with 1 because the partition is using the segment.
    private volatile int ownershipCount = 1;

    private final RecordEncoder encoder;
    private final SerializationService serializationService;
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final RecordModel recordModel;
    private final long startNanos = System.nanoTime();
    private final long maxSegmentSize;
    private final long indicesAddress;
    private final Map<String, Aggregator> aggregators;
    private long lastInsertNanos = startNanos;
    private long dataAddress;
    private long segmentSize;
    private long index = 0;

    public Segment(long initialSegmentSize,
                   long maxSegmentSize,
                   SerializationService serializationService,
                   RecordModel recordModel,
                   RecordEncoder encoder,
                   Map<String, Aggregator> aggregators) {
        this.recordModel = recordModel;
        this.maxSegmentSize = maxSegmentSize;
        this.serializationService = serializationService;
        this.aggregators = aggregators;

        this.indicesAddress = recordModel.getIndexSize() == 0 ? 0 : unsafe.allocateMemory(recordModel.getIndexSize());
        if (indicesAddress != 0) {
            // set -1 on each bucket in the index
            for (int k = 0; k < recordModel.getIndexSize() / 4; k++) {
                unsafe.putInt(indicesAddress + k * 4, -1);
            }
        }
        this.dataAddress = unsafe.allocateMemory(initialSegmentSize);
        this.segmentSize = initialSegmentSize;
        this.encoder = encoder;
    }

    public boolean acquire() {
        for (; ; ) {
            int currentUsed = ownershipCount;

            if (currentUsed == 0) {
                // the segment has been destroyed.
                return false;
            }

            int newUsed = currentUsed + 1;
            if (OWNERSHIP_COUNT.compareAndSet(this, currentUsed, newUsed)) {
                return true;
            }
        }
    }

    public void release() {
        for (; ; ) {
            int currentUsed = ownershipCount;
            int newUsed = currentUsed - 1;
            if (OWNERSHIP_COUNT.compareAndSet(this, currentUsed, newUsed)) {
                if (newUsed == 0) {
                    destroy0();
                }
                return;
            }
        }
    }

    public long count() {
        return index;
    }

    public long indicesAddress() {
        return indicesAddress;
    }

    public long dataAddress() {
        return dataAddress;
    }

    public Map<String, Aggregator> getAggregators() {
        return aggregators;
    }

    public long firstInsertNanos() {
        return startNanos;
    }

    public long lastInsertNanos() {
        return lastInsertNanos;
    }

    public long allocatedBytes() {
        return segmentSize;
    }

    public long consumedBytes() {
        // todo: the indices are not included
        return index * recordModel.getSize();
    }

    public void insert(Data keyData, Object valueData) {
        Object record = serializationService.toObject(valueData);
        if (record.getClass() != recordModel.getRecordClass()) {
            throw new RuntimeException(format("Expected value of class '%s', but found '%s' ",
                    record.getClass().getName(), recordModel.getRecordClass().getClass().getName()));
        }

        int recordOffset = (int) (index * recordModel.getSize());
        encoder.writeRecord(record, dataAddress, recordOffset, indicesAddress);
        for (Aggregator aggregator : aggregators.values()) {
            aggregator.accumulate(record);
        }
        index++;
        lastInsertNanos = System.nanoTime();
    }

    public boolean ensureCapacity() {
        long requiredSegmentSize = (index + 1) * recordModel.getSize();

        if (requiredSegmentSize < segmentSize) {
            return true;
        }

        if (maxSegmentSize <= requiredSegmentSize) {
            // we can't grow any further.
            return false;
        }

        long newSegmentSize = Math.min(maxSegmentSize, 2 * this.segmentSize);
        System.out.println("Growing segment from:" + this.segmentSize + " to:" + newSegmentSize);

        long newPointer = unsafe.allocateMemory(newSegmentSize);

        unsafe.copyMemory(dataAddress, newPointer, segmentSize);
        unsafe.freeMemory(dataAddress);

        this.segmentSize = newSegmentSize;
        this.dataAddress = newPointer;
        return true;
    }

    public void destroy() {
        release();
    }

    // does the actual destruction; will only be done where the last user ends using the segment
    private void destroy0() {
        System.out.println("Destroying segment");

        unsafe.freeMemory(dataAddress);
        if (indicesAddress != 0) {
            unsafe.freeMemory(indicesAddress);
        }
    }
}
