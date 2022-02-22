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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.security.permission.ConnectorPermission;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @see SourceProcessors#streamSocketP(String, int, Charset)
 */
public final class StreamSocketP extends AbstractProcessor {

    private static final int BUFFER_SIZE = 4096;
    private static final int MAX_BYTES_PER_CHAR = 4;

    private final String host;
    private final int port;
    private final CharsetDecoder charsetDecoder;
    private final StringBuilder lineBuilder = new StringBuilder();
    private String pendingLine;
    private SocketChannel socketChannel;
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
    private final CharBuffer charBuffer = CharBuffer.allocate(BUFFER_SIZE);
    private boolean socketDone;
    private boolean maybeLfExpected;

    public StreamSocketP(String host, int port, Charset charset) {
        this.host = host;
        this.port = port;
        this.charsetDecoder = charset.newDecoder();
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        getLogger().info("Connecting to socket " + hostAndPort());
        socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(new InetSocketAddress(host, port));
        // block until connection is finished
        while (!socketChannel.finishConnect()) {
            LockSupport.parkNanos(MILLISECONDS.toNanos(1));
        }
        getLogger().info("Connected to socket " + hostAndPort());
        upcast(byteBuffer).limit(0);
        upcast(charBuffer).limit(0);
    }

    @Override
    public boolean complete() {
        try {
            return tryComplete();
        } catch (Exception e) {
            throw sneakyThrow(e);
        }
    }

    private boolean tryComplete() throws IOException {
        fillCharBuffer();
        emitFromCharBuffer();

        return socketDone && pendingLine == null;
    }

    private void fillCharBuffer() throws IOException {
        if (socketDone || charBuffer.hasRemaining()) {
            return;
        }
        socketDone = socketChannel.read(byteBuffer) < 0;
        upcast(byteBuffer).flip();
        upcast(charBuffer).clear();
        charsetDecoder.decode(byteBuffer, charBuffer, socketDone);
        upcast(charBuffer).flip();
        byteBuffer.compact();
        assert byteBuffer.position() < MAX_BYTES_PER_CHAR - 1 : "position=" + byteBuffer.position();
    }

    private void emitFromCharBuffer() {
        while (charBuffer.hasRemaining()) {
            if (pendingLine == null) {
                pendingLine = tryReadLineFromBuffer();
            }
            if (pendingLine != null) {
                if (tryEmit(pendingLine)) {
                    pendingLine = null;
                } else {
                    break;
                }
            }
        }
    }

    private String tryReadLineFromBuffer() {
        while (charBuffer.hasRemaining()) {
            char ch = charBuffer.get();
            if (ch == '\r' || ch == '\n') {
                // Handle line ending
                if (maybeLfExpected && ch == '\n') {
                    maybeLfExpected = false;
                    continue;
                }
                if (ch == '\r') {
                    maybeLfExpected = true;
                }
                try {
                    return lineBuilder.toString();
                } finally {
                    lineBuilder.setLength(0);
                }
            } else {
                // Handle line content
                lineBuilder.append(ch);
                maybeLfExpected = false;
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (socketChannel != null) {
            getLogger().info("Closing socket " + hostAndPort());
            socketChannel.close();
        }
    }

    private String hostAndPort() {
        return host + ':' + port;
    }

    /**
     * Internal API, use {@link SourceProcessors#streamSocketP(String, int, Charset)}.
     */
    public static ProcessorMetaSupplier supplier(String host, int port, @Nonnull String charset) {
        return ProcessorMetaSupplier.preferLocalParallelismOne(
                ConnectorPermission.socket(host, port, ACTION_READ),
                SecuredFunctions.streamSocketProcessorFn(host, port, charset)
        );
    }
}
