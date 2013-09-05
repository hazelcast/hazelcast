/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.ssl;

import com.hazelcast.nio.DefaultSocketChannelWrapper;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SSLSocketChannelWrapper extends DefaultSocketChannelWrapper {

    private final ByteBuffer in;
    private final ByteBuffer emptyBuffer;
    private final ByteBuffer netOutBuffer;      // "reliable" write transport
    private final ByteBuffer netInBuffer;      // "reliable" read transport
    private final SSLEngine sslEngine;
    private volatile boolean handshake = false;
    private SSLEngineResult sslEngineResult;

    public SSLSocketChannelWrapper(SSLContext sslContext, SocketChannel sc, boolean client) throws Exception {
        super(sc);
        sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(client);
        sslEngine.setEnableSessionCreation(true);
        SSLSession session = sslEngine.getSession();
        in = ByteBuffer.allocate(64 * 1024);
        emptyBuffer = ByteBuffer.allocate(0);
        int netBufferMax = session.getPacketBufferSize();
        netOutBuffer = ByteBuffer.allocate(netBufferMax);
        netInBuffer = ByteBuffer.allocate(netBufferMax);
    }

    private void handshake() throws IOException {
        synchronized (this) {
            if (handshake) {
                return;
            }
            writeInternal(emptyBuffer);
            while (sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                    netInBuffer.clear();

                    while (socketChannel.read(netInBuffer) < 1) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            throw new IOException(e);
                        }
                    }
                    netInBuffer.flip();
                    unwrap(netInBuffer);

                    if (sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                        writeInternal(emptyBuffer);
                    }
                } else if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                    writeInternal(emptyBuffer);
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
            }
            in.clear();
            in.flip();
            handshake = true;
        }
    }

    private ByteBuffer unwrap(ByteBuffer b) throws SSLException {
        in.clear();
        while (b.hasRemaining()) {
            sslEngineResult = sslEngine.unwrap(b, in);
            if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                Runnable task;
                while ((task = sslEngine.getDelegatedTask()) != null) {
                    task.run();
                }
            } else if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED
                    || sslEngineResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                return in;
            }
        }
        in.flip();
        return in;
    }

    public int write(ByteBuffer input) throws IOException {
        if (!handshake) {
            handshake();
        }
        return writeInternal(input);
    }

    private int writeInternal(ByteBuffer input) throws IOException {
        sslEngineResult = sslEngine.wrap(input, netOutBuffer);
        netOutBuffer.flip();
        int written = socketChannel.write(netOutBuffer);
        if (netOutBuffer.hasRemaining()) {
            netOutBuffer.compact();
        } else {
            netOutBuffer.clear();
        }
        return written;
    }

    public int read(ByteBuffer output) throws IOException {
        if (!handshake) {
            handshake();
        }
        int readBytesCount = 0;
        int limit;
        if (in.hasRemaining()) {
            limit = Math.min(in.remaining(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(in.get());
                readBytesCount++;
            }
            return readBytesCount;
        }
        if (netInBuffer.hasRemaining()) {
            unwrap(netInBuffer);
            limit = Math.min(in.limit(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(in.get());
                readBytesCount++;
            }
            if (sslEngineResult.getStatus() != SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                netInBuffer.clear();
                netInBuffer.flip();
                return readBytesCount;
            }
        }
        if (netInBuffer.hasRemaining()) {
            netInBuffer.compact();
        } else {
            netInBuffer.clear();
        }
        if (socketChannel.read(netInBuffer) == -1) {
            netInBuffer.clear();
            netInBuffer.flip();
            return -1;
        }
        netInBuffer.flip();
        unwrap(netInBuffer);
        limit = Math.min(in.limit(), output.remaining());
        for (int i = 0; i < limit; i++) {
            output.put(in.get());
            readBytesCount++;
        }
        return readBytesCount;
    }

    public void close() throws IOException {
        sslEngine.closeOutbound();
        try {
            write(emptyBuffer);
        } catch (Exception ignored) {
        }
        socketChannel.close();
    }

    @Override
    public long read(ByteBuffer[] byteBuffers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] byteBuffers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] byteBuffers, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] byteBuffers, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SSLSocketChannelWrapper{");
        sb.append("socketChannel=").append(socketChannel);
        sb.append('}');
        return sb.toString();
    }
}
