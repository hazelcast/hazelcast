/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.iobuffer;


import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpmcArrayQueue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link IOBufferAllocator} that can be used in parallel by multiple threads.
 * <p>
 * It also allows the {@link #allocate()} of a {@link IOBuffer} to be done by a different
 * thread than {@link #free(IOBuffer)}.
 */
@SuppressWarnings({"checkstyle:MagicNumber", "java:S1068", "java:S125", "java:S112"})
public class ConcurrentIOBufferAllocator implements IOBufferAllocator {

    // this is for debugging. We do not want to track these calls normally
    private static final AtomicLong NEW_ALLOCATIONS = new AtomicLong();
    private static final AtomicLong POOLED_ALLOCATIONS = new AtomicLong();
    private static final AtomicLong ALLOCATE_CALLS = new AtomicLong();
    private static final AtomicLong RELEASE_CALLS = new AtomicLong();

    @SuppressWarnings("java:S5164")
    private static final ThreadLocal<Pool> POOL = new ThreadLocal<>();

    private final MpmcArrayQueue<IOBuffer> queue = new MpmcArrayQueue<>(4096);
    private final boolean direct;

    static class Pool {
        private long newAllocateCnt;
        private long allocateCnt;
        private IOBuffer[] bufs = new IOBuffer[128];
        private int index = -1;
        private final MessagePassingQueue.Consumer<IOBuffer> consumer = buf -> {
            index++;
            bufs[index] = buf;
        };
    }

    private final int minSize;

    public ConcurrentIOBufferAllocator(int minSize, boolean direct) {
        this.minSize = minSize;
        this.direct = direct;
    }

    @Override
    public IOBuffer allocate(int minSize) {
        IOBuffer buf = allocate();
        buf.ensureRemaining(minSize);
        return buf;
    }

    @SuppressWarnings("java:S3776")
    @Override
    public IOBuffer allocate() {
        Pool pool = POOL.get();
        if (pool == null) {
            pool = new Pool();
            POOL.set(pool);
        }
        pool.allocateCnt++;
        if (pool.index == -1) {
            // the pool is empty.

            int count = 0;
            for (int k = 0; k < pool.bufs.length; k++) {
                IOBuffer buf = queue.poll();
                if (buf == null) {
                    break;
                }
                count++;
                pool.index++;
                pool.bufs[pool.index] = buf;
            }

            // Lets gets some bufs from the queue.
            //int count = queue.drain(pool.consumer, pool.bufs.length);

            // and lets create a bunch of bufs ourselves so we don't end up
            // continuously asking the queue for requests.
            for (int k = count; k < pool.bufs.length; k++) {
                //newAllocations.incrementAndGet();
                //System.out.println(" new buf");
                ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(minSize) : ByteBuffer.allocate(minSize);
                IOBuffer buf = new IOBuffer(buffer);
                buf.concurrent = true;
                buf.allocator = this;
                pool.newAllocateCnt++;
                pool.index++;
                pool.bufs[k] = buf;
            }
        }

//        if (pool.allocateCnt % 1_000_000 == 0) {
//            System.out.println("New allocate percentage:" + (pool.newAllocateCnt * 100f) / pool.allocateCnt + "%");
//        }

        IOBuffer buf = pool.bufs[pool.index];
        pool.bufs[pool.index] = null;
        pool.index--;

        // for debugging; it should not be needed. We should be able to do a lazySet 1
        for (; ; ) {
            int c = buf.refCount.get();
            if (c != 0) {
                throw new RuntimeException("Ref count should be 0, but was: " + buf.refCount());
            }

            if (buf.refCount.compareAndSet(0, 1)) {
                break;
            }
        }
        return buf;
    }

    @Override
    public void free(IOBuffer buf) {
        if (buf.refCount.get() != 0) {
            throw new RuntimeException("refCount should be 0, but was:" + buf.refCount.get());
        }

        buf.clear();
        buf.next = null;

        Pool pool = POOL.get();
        if (pool == null) {
            // if pool == null:
            // if the thread has never allocated anything, we don't want to create
            // a pool for that thread.
            queue.offer(buf);
        } else if (pool.index == pool.bufs.length - 1) {
            // the pool of the object is full, so lets toss it in the queue.
            queue.offer(buf);
        } else {
            // there is space in the pool.
            pool.index++;
            pool.bufs[pool.index] = buf;
        }
    }
}
