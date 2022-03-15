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

package com.hazelcast.internal.util;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.System.arraycopy;

/**
 * {@link InputStream} implementation with a configurable buffer.
 */
public class BufferingInputStream extends InputStream {

    private static final int BYTE_MASK = 0xff;

    private final InputStream in;
    private final byte[] buf;

    private int position;
    private int limit;

    public BufferingInputStream(InputStream in, int bufferSize) {
        this.in = in;
        this.buf = new byte[bufferSize];
    }

    @Override
    public int read() throws IOException {
        if (!ensureDataInBuffer()) {
            return -1;
        }
        return buf[position++] & BYTE_MASK;
    }

    @Override
    @SuppressWarnings("NullableProblems")
    public int read(byte[] destBuf, int off, int len) throws IOException {
        if (!ensureDataInBuffer()) {
            return -1;
        }
        int transferredCount = Math.min(limit - position, len);
        arraycopy(buf, position, destBuf, off, transferredCount);
        position += transferredCount;
        return transferredCount;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    private boolean ensureDataInBuffer() throws IOException {
        if (position != limit) {
            return true;
        }
        position = 0;
        final int newLimit = in.read(buf);
        if (newLimit == -1) {
            limit = 0;
            return false;
        } else {
            limit = newLimit;
            return true;
        }
    }
}
