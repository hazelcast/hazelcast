/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.logstore;


import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

/**
 * Segments can be used by multiple threads concurrently since it is immutable. Before a
 * thread wants to use it, it needs to call acquire and on completion it needs to call release.
 */
class Segment {

    private static final AtomicIntegerFieldUpdater<Segment> OWNERS = newUpdater(Segment.class, "owners");
    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    volatile Segment next;
    volatile Segment prev;
    volatile int owners = 1;

    final long address;
    final int limit;
    final long startSequence;
    final long count;
    final long fromNanos;
    final long toNanos;

    public Segment(long address, int limit, long startSequence, long count, long fromNanos, long toNanos) {
        this.address = address;
        this.limit = limit;
        this.startSequence = startSequence;
        this.count = count;
        this.fromNanos = fromNanos;
        this.toNanos = toNanos;
    }

    /**
     * Acquires this segment. This is needed before the segment is used.
     *
     * This method is thread-safe.
     *
     * @return true if this segment was acquired successfully. If false is returned, this segment can never
     * be acquired again.
     * @see #release()
     */
    public boolean acquire() {
        for (; ; ) {
            int oldOwners = owners;
            if (oldOwners == 0) {
                return false;
            }
            int newOwners = oldOwners + 1;
            if (OWNERS.compareAndSet(this, oldOwners, newOwners)) {
                return true;
            }
        }
    }

    /**
     * Releases the Segment.
     *
     * The Segment makes use of a smart-pointer to allow for a segment to be used by multiple threads.
     *
     * Once the Segment has 0 owners, it can never be acquired again.
     *
     * This method is thread-safe.
     *
     * @see #acquire()
     */
    public void release() {
        for (; ; ) {
            int oldOwners = owners;
            if (oldOwners == 0) {
                throw new IllegalStateException();
            }

            int newOwners = oldOwners - 1;
            if (OWNERS.compareAndSet(this, oldOwners, newOwners)) {
                if (newOwners == 0) {
                    // we are the last one using this region, so we can write to file
                    // and release the memory.
                    //saveToFile();
                    UNSAFE.freeMemory(address);
                }
                return;
            }
        }
    }
}
