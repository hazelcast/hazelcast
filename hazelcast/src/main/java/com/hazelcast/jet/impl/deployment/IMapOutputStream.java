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

package com.hazelcast.jet.impl.deployment;

import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.hazelcast.internal.util.JVMUtil.upcast;
import static java.lang.Math.min;

public class IMapOutputStream extends OutputStream {
    private static final int CHUNK_SIZE = 1 << 17;

    private final String prefix;
    private final IMap<String, byte[]> map;
    private final ByteBuffer currentChunk = ByteBuffer.allocate(CHUNK_SIZE);
    private final byte[] singleByteBuffer = new byte[1];

    private int currentChunkIndex;

    public IMapOutputStream(IMap<String, byte[]> map, String prefix) {
        this.map = map;
        this.prefix = prefix;
    }

    @Override
    public void close() throws IOException {
        if (isClosed()) {
            return;
        }
        flush();
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        buf.putInt(currentChunkIndex);
        map.put(prefix, buf.array());
        currentChunkIndex = -1;
    }

    private boolean isClosed() {
        return currentChunkIndex < 0;
    }

    @Override
    public void write(@Nonnull byte[] b, int off, int len) throws IOException {
        if ((len | off) < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException(String.format(
                    "b.length == %,d, off == %,d, len == %,d", b.length, off, len));
        }
        if (isClosed()) {
            throw new IOException("Stream already closed");
        }
        for (int writeCount = 0; writeCount < len; ) {
            int countToPut = min(len - writeCount, currentChunk.remaining());
            currentChunk.put(b, off + writeCount, countToPut);
            writeCount += countToPut;
            if (currentChunk.remaining() == 0) {
                flush();
            }
        }
    }

    @Override
    public void write(int b) throws IOException {
        singleByteBuffer[0] = (byte) b;
        write(singleByteBuffer);
    }

    @Override
    public void flush() throws IOException {
        if (isClosed()) {
            return;
        }
        byte[] value = currentChunk.array();
        if (currentChunk.remaining() > 0) {
            // if we have remaining capacity, we need to truncate the value
            value = Arrays.copyOf(value, currentChunk.position());
        }
        try {
            map.put(prefix + '_' + currentChunkIndex, value);
        } catch (Exception e) {
            throw new IOException("Writing to chunked IMap failed: " + e, e);
        }
        currentChunkIndex++;
        upcast(currentChunk).clear();
    }
}
