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

package com.hazelcast.internal.tpc.iobuffer;

import com.hazelcast.internal.tpc.util.BufferUtil;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.tpc.util.BufferUtil.allocateDirectAligned;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;
import static java.nio.ByteBuffer.allocateDirect;

/**
 * A {@link IOBufferAllocator} that can only be used serially (so by a single thread).
 * <p>
 * {@link #allocate()} should be done by the same thread as {@link #free(IOBuffer)}.
 */
public final class NonConcurrentIOBufferAllocator implements IOBufferAllocator {
    private final int minSize;
    private final boolean direct;
    private final int alignment;
    private long newAllocateCnt = 0;
    private long allocateCnt = 0;
    private IOBuffer[] bufs = new IOBuffer[512];
    private int index = -1;

    public NonConcurrentIOBufferAllocator(int minSize, boolean direct) {
        this(minSize, direct, 1);
    }

    public NonConcurrentIOBufferAllocator(int minSize, boolean direct, int alignment) {
        this.minSize = checkNotNegative(minSize, "minSize");
        this.direct = direct;
        this.alignment = checkPositive(alignment, "alignment");
    }

    @Override
    public IOBuffer allocate() {
        allocateCnt++;

        if (index == -1) {
            // the pool is empty.
            // and lets create a set of bufs so we don't end up
            // continuously asking the queue for ones.
            for (int k = 0; k < bufs.length; k++) {
                //newAllocations.incrementAndGet();
                //System.out.println(" new buf");
                ByteBuffer buffer;
                if (direct) {
                    if (alignment == 1) {
                        buffer = allocateDirect(minSize);
                    } else {
                        buffer = allocateDirectAligned(minSize, alignment);
                    }
                } else {
                    buffer = ByteBuffer.allocate(minSize);
                }

                IOBuffer buf = new IOBuffer(buffer);
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

        IOBuffer buf = bufs[index];
        bufs[index] = null;
        index--;
        buf.acquire();
        return buf;
    }

    @Override
    public IOBuffer allocate(int minSize) {
        IOBuffer buf = allocate();
        buf.ensureRemaining(minSize);
        return buf;
    }

    @Override
    public void free(IOBuffer buf) {
        buf.clear();
        buf.next = null;

        if (index == bufs.length - 1) {
            IOBuffer[] newBuf = new IOBuffer[bufs.length * 2];
            System.arraycopy(bufs, 0, newBuf, 0, bufs.length);
            bufs = newBuf;
        }

        index++;
        bufs[index] = buf;
    }
}
