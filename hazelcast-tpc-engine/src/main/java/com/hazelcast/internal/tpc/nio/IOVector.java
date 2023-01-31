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

import com.hazelcast.internal.tpc.buffer.Buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Queue;

import static com.hazelcast.internal.tpc.util.BitUtil.nextPowerOfTwo;

/**
 * Contains logic to do vectorized I/O (so instead of passing a single buffer, an array of buffer is passed to socket.write).
 */
public final class IOVector {

    private static final int IOV_MAX = 1024;

    private final Buffer[] buffers = new Buffer[IOV_MAX];
    private ByteBuffer[] byteBuffers = new ByteBuffer[IOV_MAX];

    private int ioBuffersSize;
    private int byteBuffersSize;
    private long pending;

    public boolean isEmpty() {
        return ioBuffersSize == 0;
    }

    public void populate(Queue<Buffer> queue) {
        int count = IOV_MAX - ioBuffersSize;
        for (int k = 0; k < count; k++) {
            Buffer buf = queue.poll();
            if (buf == null) {
                break;
            }
            add0(buf);
        }
    }

    public boolean offer(Buffer buf) {
        if (ioBuffersSize == IOV_MAX) {
            return false;
        } else {
            add0(buf);
            return true;
        }
    }

    private void add0(Buffer buf) {
        buffers[ioBuffersSize] = buf;
        ioBuffersSize++;
        ByteBuffer[] chunks = buf.getChunks();
        ensureRemaining(chunks.length);
        System.arraycopy(chunks, 0, byteBuffers, byteBuffersSize, chunks.length);
        byteBuffersSize += chunks.length;
        pending += buf.remaining();
    }

    public long write(SocketChannel socketChannel) throws IOException {
        long written;
        if (byteBuffersSize == 1) {
            written = socketChannel.write(byteBuffers[0]);
        } else {
            written = socketChannel.write(byteBuffers, 0, byteBuffersSize);
        }
        compact(written);
        return written;
    }

    void compact(long written) {
        if (written == pending) {
            // everything was written
            for (int i = 0; i < ioBuffersSize; i++) {
                buffers[i].release();
                buffers[i] = null;
            }
            for (int i = 0; i < byteBuffersSize; i++) {
                byteBuffers[i] = null;
            }
            byteBuffersSize = 0;
            ioBuffersSize = 0;
            pending = 0;
            return;
        }

        // not everything was written
        int toIndexByteBuffers = 0;
        int initialByteBuffersLength = byteBuffersSize;
        int chunkOwnerPos = 0;

        for (int i = 0; i < initialByteBuffersLength; i++) {
            if (byteBuffers[i].hasRemaining()) {
                if (i == 0) {
                    // the first one is not empty, we are done
                    break;
                } else {
                    byteBuffers[toIndexByteBuffers] = byteBuffers[i];
                    byteBuffers[i] = null;
                    toIndexByteBuffers++;
                }
            } else {
                byteBuffersSize--;
                buffers[chunkOwnerPos].releaseNextChunk(byteBuffers[i]);
                if (!buffers[chunkOwnerPos].hasRemainingChunks()) {
                    chunkOwnerPos++;
                }
                byteBuffers[i] = null;
            }
        }

        int toIndexIoBuffers = 0;
        int initialIOBuffersLength = ioBuffersSize;
        for (int i = 0; i < initialIOBuffersLength; i++) {
            if (buffers[i].hasRemainingChunks()) {
                if (i == 0) {
                    // the first one is not empty, we are done
                    break;
                } else {
                    buffers[toIndexIoBuffers] = buffers[i];
                    buffers[i] = null;
                    toIndexIoBuffers++;
                }
            } else {
                ioBuffersSize--;
                buffers[i].release();
                buffers[i] = null;
            }
        }
    }

    private void ensureRemaining(int size) {
        if (byteBuffers.length >= byteBuffersSize + size) {
            return;
        }
        int nextSize = nextPowerOfTwo(byteBuffersSize + size);
        byteBuffers = Arrays.copyOf(byteBuffers, nextSize);
    }
}
