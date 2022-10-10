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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.iobuffer.IOBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;

import static com.hazelcast.internal.tpc.iouring.Linux.IOV_MAX;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * Contains logic to do vectorized I/O (so instead of passing a single buffer, an array of buffer is passed to socket.write).
 */
public final class IOVector {

    private final ByteBuffer[] byteBufs;
    private final IOBuffer[] ioBufs;
    private final int capacity;
    private int count;
    private long pending;

    public IOVector(){
        this(IOV_MAX);
    }

    public IOVector(int capacity){
        this.capacity = checkPositive(capacity, "capacity");
        if (capacity > IOV_MAX) {
            throw new IllegalArgumentException("capacity can't be larger than IOV_MAX=" + IOV_MAX);
        }
        this.byteBufs = new ByteBuffer[capacity];
        this.ioBufs = new IOBuffer[capacity];
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public int count() {
        return count;
    }

    public void populate(Queue<IOBuffer> queue) {
        int available = capacity - count;
        for (int k = 0; k < available; k++) {
            IOBuffer buf = queue.poll();
            if (buf == null) {
                break;
            }

            ByteBuffer byteBuf = buf.byteBuffer();
            byteBufs[count] = byteBuf;
            ioBufs[count] = buf;
            count++;
            pending += byteBuf.remaining();
        }
    }

    public boolean offer(IOBuffer buf) {
        if (count == capacity) {
            return false;
        } else {
            ByteBuffer byteBuf = buf.byteBuffer();
            byteBufs[count] = byteBuf;
            ioBufs[count] = buf;
            count++;
            pending += byteBuf.remaining();
            return true;
        }
    }

    public long write(SocketChannel socketChannel) throws IOException {
        long written;
        if (count == 1) {
            written = socketChannel.write(byteBufs[0]);
        } else {
            written = socketChannel.write(byteBufs, 0, count);
        }
        compact(written);
        return written;
    }

    void compact(long written) {
        if (written == pending) {
            // everything was written
            for (int k = 0; k < count; k++) {
                byteBufs[k] = null;
                ioBufs[k].release();
                ioBufs[k] = null;
            }
            count = 0;
            pending = 0;
        } else {
            // not everything was written
            int toIndex = 0;
            int oldCount = count;
            for (int index = 0; index < oldCount; index++) {
                if (!byteBufs[index].hasRemaining()) {
                    // The buffer was completely written
                    count--;

                    // the buffer can now be removed.
                    byteBufs[index] = null;
                    ioBufs[index].release();
                    ioBufs[index] = null;
                } else {
                    // the buffer was not completely written
                    if (index == 0) {
                        // the first one is not empty, we are done. No need for compaction
                        break;
                    } else {
                        // compaction is needed.
                        byteBufs[toIndex] = byteBufs[index];
                        byteBufs[index] = null;
                        ioBufs[toIndex] = ioBufs[index];
                        ioBufs[index] = null;
                        toIndex++;
                    }
                }
            }
            pending -= written;
        }
    }
}
