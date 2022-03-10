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

/**
 * Contains logic to do vectorized I/O (so instead of passing a single buffer, an array of buffer is passed).
 */
public final class IOVector {

    private final static int IOV_MAX = 1024;

    private final ByteBuffer[] array = new ByteBuffer[IOV_MAX];
    private final IOBuffer[] bufs = new IOBuffer[IOV_MAX];
    private int size = 0;
    private long pending;

    public boolean isEmpty() {
        return size == 0;
    }

    public void fill(Queue<IOBuffer> queue) {
        int count = IOV_MAX - size;
        for (int k = 0; k < count; k++) {
            IOBuffer buf = queue.poll();
            if (buf == null) {
                break;
            }

            ByteBuffer buffer = buf.byteBuffer();
            array[size] = buffer;
            bufs[size] = buf;
            size++;
            pending += buffer.remaining();
        }
    }

    public boolean add(IOBuffer buf) {
        if (size == IOV_MAX) {
            return false;
        } else {
            ByteBuffer buffer = buf.byteBuffer();
            array[size] = buffer;
            bufs[size] = buf;
            size++;
            pending += buffer.remaining();
            return true;
        }
    }

    public long write(SocketChannel socketChannel) throws IOException {
        long written;
        if (size == 1) {
            written = socketChannel.write(array[0]);
        } else {
            written = socketChannel.write(array, 0, size);
        }
        compact(written);
        return written;
    }

    void compact(long written) {
        if (written == pending) {
            for (int k = 0; k < size; k++) {
                array[k] = null;
                bufs[k].release();
                bufs[k] = null;
            }
            size = 0;
            pending = 0;
        } else {
            int toIndex = 0;
            int length = size;
            for (int k = 0; k < length; k++) {
                if (array[k].hasRemaining()) {
                    if (k == 0) {
                        // the first one is not empty, we are done
                        break;
                    } else {
                        array[toIndex] = array[k];
                        array[k] = null;
                        bufs[toIndex] = bufs[k];
                        bufs[k] = null;
                        toIndex++;
                    }
                } else {
                    size--;
                    array[k] = null;
                    bufs[k].release();
                    bufs[k] = null;
                }
            }
            pending -= written;
        }
    }

    public int size() {
        return size;
    }
}
