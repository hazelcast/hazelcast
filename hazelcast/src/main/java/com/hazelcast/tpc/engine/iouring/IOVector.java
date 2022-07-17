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

package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.tpc.engine.iobuffer.IOBuffer;
import io.netty.channel.unix.IovArray;

import java.nio.ByteBuffer;
import java.util.Queue;

// todo: instead of an 'array' we could use a ring so we don't need to copy to an earlier position
// TODO: This class assumes direct byte buffers. For future safety we should also allow for non direct
public final class IOVector {

    private final static int IOV_MAX = 1024;

    private final IOBuffer[] array = new IOBuffer[IOV_MAX];
    private int size = 0;
    private long pending;

    public boolean isEmpty() {
        return size == 0;
    }

    public IOBuffer get(int index) {
        return array[index];
    }

    public void fill(Queue<IOBuffer> queue) {
        int count = IOV_MAX - size;
        for (int k = 0; k < count; k++) {
            IOBuffer buf = queue.poll();
            if (buf == null) {
                break;
            }
            ByteBuffer buffer = buf.byteBuffer();
            array[size] = buf;
            size++;
            pending += buffer.remaining();
        }
    }

    public void fillIoArray(IovArray iovArray) {
        for (int k = 0; k < size; k++) {
            IOBuffer buf = array[k];
            ByteBuffer byteBuffer = buf.byteBuffer();
            iovArray.add(byteBuffer, byteBuffer.remaining());
        }
    }

    public boolean add(IOBuffer buf) {
        if (size == IOV_MAX) {
            return false;
        } else {
            ByteBuffer buffer = buf.byteBuffer();
            array[size] = buf;
            size++;
            pending += buffer.remaining();
            return true;
        }
    }

    public void compact(long written) {
        if (written == pending) {
            for (int k = 0; k < size; k++) {
                array[k].release();
                array[k] = null;
            }
            size = 0;
            pending = 0;
        } else {
            long w = written;
            int toIndex = 0;
            int cachedSize = size;
            for (int k = 0; k < cachedSize; k++) {
                IOBuffer buf = array[k];
                ByteBuffer byteBuffer = buf.byteBuffer();
                int bufferRemaining = byteBuffer.remaining();
                if (w < bufferRemaining) {
                    byteBuffer.position(byteBuffer.position() + (int) w);
                    if (k == 0) {
                        // the first one is not empty, we are done
                        break;
                    } else {
                        array[toIndex] = array[k];
                        array[k] = null;
                        toIndex++;
                    }
                } else {
                    w -= bufferRemaining;
                    size--;
                    buf.release();
                    array[k] = null;
                }
            }

            pending -= written;
        }
    }

    public int size() {
        return size;
    }
}
