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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import static java.lang.Math.min;

public class IMapInputStream extends InputStream {

    private final IMap<String, byte[]> map;
    private final String prefix;
    private final int chunkCount;

    private ByteBuffer currentChunk;
    private int currentChunkIndex;

    public IMapInputStream(IMap<String, byte[]> map, String prefix) {
        this.map = map;
        this.prefix = prefix;
        byte[] encodedChunkCount = Objects.requireNonNull(map.get(prefix),
                "A file/directory with id '" + prefix + "' wasn't attached");
        this.chunkCount = ByteBuffer.wrap(encodedChunkCount).getInt();
    }

    @Override
    public void close() throws IOException {
        currentChunk = null;
        currentChunkIndex = -1;
    }

    private boolean isClosed() {
        return currentChunkIndex < 0;
    }

    @Override
    public int read(@Nonnull byte[] b, int off, int len) throws IOException {
        if ((len | off) < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException(String.format(
                    "b.length == %,d, off == %,d, len == %,d", b.length, off, len));
        }
        if (isClosed()) {
            throw new IOException("Stream already closed");
        }
        try {
            if (currentChunkIndex == 0 && !fetchNextChunk()) {
                return -1;
            }
            int readCount = 0;
            do {
                int countToGet = min(len - readCount, currentChunk.remaining());
                currentChunk.get(b, off + readCount, countToGet);
                readCount += countToGet;
            } while (readCount < len && fetchNextChunk());
            return readCount > 0 ? readCount : -1;
        } catch (Exception e) {
            throw new IOException("Reading chunked IMap failed: " + e, e);
        }
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("Single-byte read not implemented");
    }

    private boolean fetchNextChunk() {
        if (currentChunkIndex == chunkCount) {
            return false;
        }
        currentChunk = ByteBuffer.wrap(map.get(prefix + '_' + currentChunkIndex));
        // Update currentChunkIndex only after map.get() succeeded
        currentChunkIndex++;
        return true;
    }
}
