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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;

import java.nio.ByteBuffer;
import java.util.Queue;

/**
 * Contains logic to do vectorized I/O (so instead of passing a single buffer, an array
 * of buffer is passed to socket.write).
 */
public final class IOVector {

    private static final int IOV_MAX = 1024;

    private final ByteBuffer[] array = new ByteBuffer[IOV_MAX];
    private final IOBuffer[] bufs = new IOBuffer[IOV_MAX];
    private int length;
    private long pending;

    public boolean isEmpty() {
        return length == 0;
    }

    public int length() {
        return length;
    }

    public ByteBuffer[] array() {
        return array;
    }

    public void populate(Queue<IOBuffer> queue) {
        int count = IOV_MAX - length;
        for (int k = 0; k < count; k++) {
            IOBuffer buf = queue.poll();
            if (buf == null) {
                break;
            }

            ByteBuffer buffer = buf.byteBuffer();
            array[length] = buffer;
            bufs[length] = buf;
            length++;
            pending += buffer.remaining();
        }
    }

    public boolean offer(IOBuffer buf) {
        if (length == IOV_MAX) {
            return false;
        } else {
            ByteBuffer buffer = buf.byteBuffer();
            array[length] = buffer;
            bufs[length] = buf;
            length++;
            pending += buffer.remaining();
            return true;
        }
    }

    public void compact(long written) {
        if (written == pending) {
            // everything was written
            for (int k = 0; k < length; k++) {
                array[k] = null;
                bufs[k].release();
                bufs[k] = null;
            }
            length = 0;
            pending = 0;
        } else {
            // not everything was written
            int toIndex = 0;
            int length0 = this.length;
            for (int k = 0; k < length0; k++) {
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
                    this.length--;
                    array[k] = null;
                    bufs[k].release();
                    bufs[k] = null;
                }
            }
            pending -= written;
        }
    }
}
