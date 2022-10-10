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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.internal.tpc.util.UnsafeLocator;

import java.nio.ByteBuffer;
import java.util.Queue;

import static com.hazelcast.internal.tpc.iouring.Linux.IOV_MAX;
import static com.hazelcast.internal.tpc.iouring.Linux.SIZEOF_IOVEC;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.BufferUtil.addressOf;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;
import static java.nio.ByteBuffer.allocateDirect;

/**
 * todo: instead of an 'array' we could use a ring so we don't need to copy to an earlier position
 * TODO: This class assumes direct byte buffers. For future safety we should also allow for non direct
 * An array of:
 * <code>
 * struct iovec {
 *   void  *iov_base;    // Starting address
 *   size_t iov_len;     // Number of bytes to transfer
 * };
 * <code>
 * <p>
 * <p>
 * The current implementation isn't the most efficient because we move all the iovs to the beginning
 * and the way this is done is by evaluating all the non empty buffers. A mem copy would be a lot faster
 */
public final class IOVector {
    private final static sun.misc.Unsafe UNSAFE = UnsafeLocator.UNSAFE;
    private final static int OFFSET_IOV_LEN = SIZEOF_LONG;//todo:

    private final IOBuffer[] ioBufs;
    private final int capacity;
    private final long addr;
    private int count = 0;
    private long pending;
    private final ByteBuffer buf;

    /**
     * Creates a new IOVector with the given capacity.
     *
     * @param capacity the capacity
     * @throws IllegalArgumentException if capacity not positive or larger than IOV_MAX.
     */
    public IOVector(int capacity) {
        this.capacity = checkPositive(capacity, "capacity");
        if (capacity > IOV_MAX) {
            throw new IllegalArgumentException("capacity can't be larger than IOV_MAX=" + IOV_MAX);
        }
        this.ioBufs = new IOBuffer[capacity];
        this.buf = allocateDirect(capacity * SIZEOF_IOVEC);
        this.addr = addressOf(buf);
    }

    /**
     * Returns the capacity of this IOVector.
     *
     * @return the capacity.
     */
    public int capacity() {
        return capacity;
    }


    public long pending() {
        return pending;
    }

    /**
     * Returns the address of the beginning of the C iovec-array.
     *
     * @return the address.
     */
    public long addr() {
        return addr;
    }

    /**
     * Check if the IOVector is empty.
     *
     * @return true when empty, false otherwise.
     */
    public boolean isEmpty() {
        return count == 0;
    }

    /**
     * Returns the number of IOBuffers currently in this IOVector.
     *
     * @return the number of IOBuffers.
     */
    public int count() {
        return count;
    }

    /**
     * Gets the IOBuffer at the given index.
     *
     * @param index
     * @return
     */
    public IOBuffer get(int index) {
        return ioBufs[index];
    }

    /**
     * Moves as many items as possible from the queue into this IOVector.
     *
     * @param queue the Queue to take IOBuffers from.
     */
    public void populate(Queue<IOBuffer> queue) {
        int available = capacity - count;
        for (int k = 0; k < available; k++) {
            IOBuffer buf = queue.poll();
            if (buf == null) {
                break;
            }
            set(count, buf);
            count++;
            pending += buf.byteBuffer().remaining();
        }
    }

    /**
     * Offers an IOBuffer to add to this IOVector.
     *
     * @param buf the IOBuffer to add.
     * @return true if the IOBuffer was successfully offered, false if there was no space.
     */
    public boolean offer(IOBuffer buf) {
        //System.out.println("offer::count:" + count + " capacity:" + capacity);
        if (count == capacity) {
            // there is no space
            return false;
        } else {
            // there is space
            set(count, buf);
            count++;
            pending += buf.byteBuffer().remaining();
            return true;
        }
    }

    /**
     * Compacts the IOVector.
     * <p>
     * It will go through the array from left to right.
     * <ol>
     * <li>It will drop all IOBuffer that have been fully written (which will be in the beginning).</li>
     * <li>As soon as an IOBuffer is found that isn't fully written, it means that the IOBuffers behind it haven't
     * been written at all. So the first non empty buffer will get its position updated and all buffers will be
     * moved to the beginning of the array</li>
     * </ol>
     *
     * @param written the number of bytes written.
     */
    public void compact(long written) {
        checkNotNegative(written, "written");

        if (written == pending) {
            // everything got written.
            // So we can release and remove all buffers
            for (int k = 0; k < count; k++) {
                ioBufs[k].release();
                set(k, null);
            }
            count = 0;
            pending = 0;
        } else {
            // not everything got written.
            long writtenSoFar = written;
            int toIndex = 0;
            int oldCount = count;
            for (int index = 0; index < oldCount; index++) {
                IOBuffer buf = ioBufs[index];
                ByteBuffer byteBuf = buf.byteBuffer();

                // io_uring hasn't update the ByteBuffer, so we can call the remaining to determine the number
                // of bytes that need to be written.
                int bufferLength = byteBuf.remaining();

                if (writtenSoFar >= bufferLength) {
                    // the buffer got fully written

                    writtenSoFar -= bufferLength;

                    // we remove the buffer from the IOVector since it has been fully written
                    set(index, null);
                    buf.release();
                    count--;
                } else {
                    // the buffer didn't get fully written.

                    // first we need to update the position because io_uring will not do that for us.
                    byteBuf.position(byteBuf.position() + (int) writtenSoFar);

                    // all written bytes have been accounted for.
                    writtenSoFar = 0;
                    if (index == 0) {
                        set(index, buf);// we need to update the length location!!!
                        // it the first buffer and not fully written, we are done. We don't need to
                        // do any compaction.
                        break;
                    } else {
                        // it isn't the first buffer, so we need to compact:shift the buffer so that
                        // all non fully written buffers are at the beginning of the array
                        set(toIndex, ioBufs[index]);
                        set(index, null);
                        toIndex++;
                    }
                }
            }

            pending -= written;
        }

        //System.out.println("Compact::count after:" + count);
    }

    private void set(int index, IOBuffer buf) {
        ioBufs[index] = buf;

        long iov_base;
        long iov_len;
        if (buf == null) {
            iov_base = 0;
            iov_len = 0;
        } else {
            ByteBuffer byteBuf = buf.byteBuffer();
            iov_base = addressOf(byteBuf) + byteBuf.position();
            iov_len = byteBuf.remaining();
        }

        long iovAddr = addr + index * SIZEOF_IOVEC;
        UNSAFE.putLong(null, iovAddr, iov_base);
        UNSAFE.putLong(null, iovAddr + OFFSET_IOV_LEN, iov_len);
    }

}
