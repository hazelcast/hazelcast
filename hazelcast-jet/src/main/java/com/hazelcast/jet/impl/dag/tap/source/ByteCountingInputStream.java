/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.dag.tap.source;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ByteCountingInputStream extends FilterInputStream {
    private long end;
    private long position;
    private long lineNumber;

    public ByteCountingInputStream(InputStream in, long end) {
        super(in);
        this.end = end;
        this.position = 0;
    }

    @Override
    public int read() throws IOException {
        if (this.position >= this.end) {
            return -1;
        }

        int byteRead = super.read();

        if (byteRead == '\n') {
            lineNumber++;
        }

        if (byteRead > 0) {
            this.position++;
        }
        return byteRead;
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (this.position >= this.end) {
            return -1;
        }

        int bytesRead = super.read(b);

        if (bytesRead > 0) {
            this.position += bytesRead;
        }

        return bytesRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (this.position >= this.end) {
            return -1;
        }

        long remaining = this.end - this.position;

        if (remaining < len) {
            len = (int) remaining;
        }

        int bytesRead = super.read(b, off, len);

        if (bytesRead > 0) {
            this.position += bytesRead;
        }

        return bytesRead;
    }

    @Override
    public long skip(long n) throws IOException {
        if (this.position >= this.end) {
            return -1L;
        }

        long skipped = super.skip(n);
        this.position += skipped;

        return skipped;
    }

    @Override
    public synchronized void mark(int readlimit) {
        return;
    }

    @Override
    public synchronized void reset() throws IOException {
        return;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    public long getLineNumber() {
        return lineNumber;
    }
}
