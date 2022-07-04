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

package com.hazelcast.config;

import com.hazelcast.internal.nio.IOUtil;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Stream wrapping or copying a generic {@link InputStream} for the
 * {@link ConfigRecognizer} API. The purpose of this class is to make
 * the provided {@link InputStream} resetable so that multiple
 * {@link ConfigRecognizer} implementations can iterate over the stream.
 * There are even basic {@link InputStream} implementations that don't
 * support {@link InputStream#reset()} such as {@link BufferedInputStream}.
 * If calling {@code reset()} on the provided implementation fails with
 * an exception, this class reads the stream into a {@code byte[]} and
 * delegates all {@link InputStream} method calls to a
 * {@link ByteArrayInputStream} created with this {@code byte[]}. To
 * prevent OOM issues, the size of this {@code byte[]} is limited to 4096
 * bytes. This limit can be configured in the constructor.
 *
 * @see ConfigRecognizer
 */
public class ConfigStream extends InputStream {
    static final int DEFAULT_READ_LIMIT_4K = 4096;

    private final InputStream delegateStream;

    public ConfigStream(InputStream configStream) throws IOException {
        this(configStream, DEFAULT_READ_LIMIT_4K);
    }

    public ConfigStream(InputStream configStream, int readLimit) throws IOException {
        this.delegateStream = useOrCopyConfigStream(configStream, readLimit);
    }

    private static InputStream useOrCopyConfigStream(InputStream configStream, int readLimit) throws IOException {
        if (!configStream.markSupported()) {
            return copyInputStream(configStream, readLimit);
        }

        try {
            configStream.reset();
            return configStream;
        } catch (Exception ex) {
            return copyInputStream(configStream, readLimit);
        }
    }

    private static InputStream copyInputStream(InputStream configStream, int readLimit) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            IOUtil.drainToLimited(configStream, baos, readLimit);
            return new ByteArrayInputStream(baos.toByteArray());
        }
    }

    @Override
    public int read() throws IOException {
        return delegateStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return delegateStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return delegateStream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegateStream.skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegateStream.available();
    }

    @Override
    public void close() throws IOException {
        delegateStream.close();
    }

    @Override
    public void mark(int readlimit) {
        delegateStream.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        delegateStream.reset();
    }

    @Override
    public boolean markSupported() {
        return delegateStream.markSupported();
    }
}
