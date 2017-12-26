/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataseries.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static java.lang.System.nanoTime;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

public class Segment {

    private final static AtomicIntegerFieldUpdater OWNERSHIP_COUNT
            = newUpdater(Segment.class, "ownershipCount");
    private final long oldSequence;

    public volatile Segment next;
    public volatile Segment previous;

    // Provides 'smart pointer' like behavior so the segment can be shared between threads once it is tenured.
    // A segment is 'immutable' from that point on, but its resources need to be released once nobody is referring
    // to the segment. Using this mechanism we can safely share segments between threads.
    // we start with 1 because the partition is using the segment.
    private volatile int ownershipCount = 1;

    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final long startNanos = nanoTime();
    private long lastInsertNanos = startNanos;
    private final long segmentSize;
    private long dataAddress;
    private long offset;
    private int size;

    public Segment(long segmentSize, long oldSequence) {
        this.segmentSize = segmentSize;
        this.dataAddress = unsafe.allocateMemory(segmentSize);
        this.oldSequence = oldSequence;
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
        return size;
    }

    public long dataAddress() {
        return dataAddress;
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
        return offset;
    }

    public long available() {
        return segmentSize - consumedBytes();
    }

    public long append(byte[] value) {
        int bytes = INT_SIZE_IN_BYTES + value.length;

        long sequence = lastSequence();

        // if(available()<)
        offset += bytes;
        size++;
        lastInsertNanos = nanoTime();
        return sequence;
    }

    public long lastSequence() {
        return oldSequence + offset;
    }

    public void destroy() {
        release();
    }

    // does the actual destruction; will only be done where the last threads
    // ends using the segment
    private void destroy0() {
        System.out.println("Destroying segment");

        unsafe.freeMemory(dataAddress);
    }
}
