/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc.iobuffer.deprecated;

import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;

import java.nio.ByteBuffer;

/**
 * A {@link IOBufferAllocator} that can only be used serially (so by a single thread).
 * <p>
 * {@link #allocate()} should be done by the same thread as {@link #free(IOBufferImpl)}.
 */
public final class NonConcurrentIOBufferAllocator implements IOBufferAllocator {
    private final int minSize;
    private final boolean direct;
    private long newAllocateCnt = 0;
    private long allocateCnt = 0;
    private IOBufferImpl[] bufs = new IOBufferImpl[4096];
    private int index = -1;

    public NonConcurrentIOBufferAllocator(int minSize, boolean direct) {
        this.minSize = minSize;
        this.direct = direct;
    }

    @Override
    public IOBufferImpl allocate() {
        allocateCnt++;

        if (index == -1) {
            // the pool is empty.
            // and lets create a set of bufs so we don't end up
            // continuously asking the queue for ones.
            for (int k = 0; k < bufs.length; k++) {
                //newAllocations.incrementAndGet();
                //System.out.println(" new buf");
                ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(minSize) : ByteBuffer.allocate(minSize);
                IOBufferImpl buf = new IOBufferImpl(buffer);
                buf.concurrent = false;
                newAllocateCnt++;
                buf.allocator = this;
                index++;
                bufs[k] = buf;
            }
        }
//
//        if (allocateCnt % 10_000_000 == 0) {
//            System.out.println("New allocate percentage:" + (newAllocateCnt * 100f) / allocateCnt + "%");
//        }

        IOBufferImpl buf = bufs[index];
        bufs[index] = null;
        index--;
        buf.acquire();
        return buf;
    }

    @Override
    public IOBufferImpl allocate(int minSize) {
        IOBufferImpl buf = allocate();
        buf.ensureRemaining(minSize);
        return buf;
    }

    @Override
    public void free(IOBufferImpl buf) {
        buf.clear();
        buf.next = null;

        if (index == bufs.length - 1) {
            IOBufferImpl[] newBuf = new IOBufferImpl[bufs.length * 2];
            System.arraycopy(bufs, 0, newBuf, 0, bufs.length);
            bufs = newBuf;
        }

        index++;
        bufs[index] = buf;
    }
}
