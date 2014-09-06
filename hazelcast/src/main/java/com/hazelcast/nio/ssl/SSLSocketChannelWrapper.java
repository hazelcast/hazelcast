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

import com.hazelcast.nio.tcp.DefaultSocketChannelWrapper;
import com.hazelcast.util.EmptyStatement;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class SSLSocketChannelWrapper extends DefaultSocketChannelWrapper {

    private static final boolean DEBUG = false;

    private final ByteBuffer applicationBuffer;
    private final Object lock = new Object();
    private final ByteBuffer emptyBuffer;
    private final ByteBuffer netOutBuffer;
    // "reliable" write transport
    private final ByteBuffer netInBuffer;
    // "reliable" read transport
    private final SSLEngine sslEngine;
    private volatile boolean handshakeCompleted;
    private SSLEngineResult sslEngineResult;

    public SSLSocketChannelWrapper(SSLContext sslContext, SocketChannel sc, boolean client) throws Exception {
        super(sc);
        sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(client);
        sslEngine.setEnableSessionCreation(true);
        SSLSession session = sslEngine.getSession();
        applicationBuffer = ByteBuffer.allocate(session.getApplicationBufferSize());
        emptyBuffer = ByteBuffer.allocate(0);
        int netBufferMax = session.getPacketBufferSize();
        netOutBuffer = ByteBuffer.allocate(netBufferMax);
        netInBuffer = ByteBuffer.allocate(netBufferMax);
    }

    /**
     * TODO sleep in sync block
     */
    private void handshake() throws IOException {
        if (handshakeCompleted) {
            return;
        }
        if (DEBUG) {
            log("Starting handshake...");
        }
        synchronized (lock) {
            if (handshakeCompleted) {
                if (DEBUG) {
                    log("Handshake already completed...");
                }
                return;
            }
            int counter = 0;
            if (DEBUG) {
                log("Begin handshake");
            }
            sslEngine.beginHandshake();
            writeInternal(emptyBuffer);
            while (counter++ < 250 && sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                if (DEBUG) {
                    log("Handshake status: " + sslEngineResult.getHandshakeStatus());
                }
                if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                    if (DEBUG) {
                        log("Begin UNWRAP");
                    }
                    netInBuffer.clear();
                    while (socketChannel.read(netInBuffer) == 0) {
                        try {
                            if (DEBUG) {
                                log("Spinning on channel read...");
                            }
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            throw new IOException(e);
                        }
                    }
                    netInBuffer.flip();
                    unwrap(netInBuffer);
                    if (DEBUG) {
                        log("Done UNWRAP: " + sslEngineResult.getHandshakeStatus());
                    }
                    if (sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                        emptyBuffer.clear();
                        writeInternal(emptyBuffer);
                        if (DEBUG) {
                            log("Done WRAP after UNWRAP: " + sslEngineResult.getHandshakeStatus());
                        }
                    }
                } else if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                    if (DEBUG) {
                        log("Begin WRAP");
                    }
                    emptyBuffer.clear();
                    writeInternal(emptyBuffer);
                    if (DEBUG) {
                        log("Done WRAP: " + sslEngineResult.getHandshakeStatus());
                    }
                } else {
                    try {
                        if (DEBUG) {
                            log("Sleeping... Status: " + sslEngineResult.getHandshakeStatus());
                        }
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
            }
            if (sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                throw new SSLHandshakeException("SSL handshake failed after " + counter
                        + " trials! -> " + sslEngineResult.getHandshakeStatus());
            }
            if (DEBUG) {
                log("Handshake completed!");
            }
            applicationBuffer.clear();
            applicationBuffer.flip();
            handshakeCompleted = true;
        }
    }

    private void log(String log) {
        if (DEBUG) {
            System.err.println(getClass().getSimpleName() + "[" + socketChannel.socket().getLocalSocketAddress() + "]: " + log);
        }
    }

    private ByteBuffer unwrap(ByteBuffer b) throws SSLException {
        applicationBuffer.clear();
        while (b.hasRemaining()) {
            synchronized (lock) {
                sslEngineResult = sslEngine.unwrap(b, applicationBuffer);
            }
            if (sslEngineResult.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
                return applicationBuffer;
            }
            if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                if (DEBUG) {
                    log("Handshake NEED TASK");
                }
                Runnable task;
                while ((task = sslEngine.getDelegatedTask()) != null) {
                    if (DEBUG) {
                        log("Running task: " + task);
                    }
                    task.run();
                }
            } else if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED
                    || sslEngineResult.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                return applicationBuffer;
            }
        }
        return applicationBuffer;
    }

    @Override
    public int write(ByteBuffer input) throws IOException {
        if (!handshakeCompleted) {
            handshake();
        }
        return writeInternal(input);
    }

    private int writeInternal(ByteBuffer input) throws IOException {
        synchronized (lock) {
            sslEngineResult = sslEngine.wrap(input, netOutBuffer);
        }
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
        if (!handshakeCompleted) {
            handshake();
        }
        int readBytesCount = 0;
        int limit;
        if (applicationBuffer.hasRemaining()) {
            limit = Math.min(applicationBuffer.remaining(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(applicationBuffer.get());
                readBytesCount++;
            }
            return readBytesCount;
        }
        if (netInBuffer.hasRemaining()) {
            unwrap(netInBuffer);
            applicationBuffer.flip();
            limit = Math.min(applicationBuffer.remaining(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(applicationBuffer.get());
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
        applicationBuffer.flip();
        limit = Math.min(applicationBuffer.remaining(), output.remaining());
        for (int i = 0; i < limit; i++) {
            output.put(applicationBuffer.get());
            readBytesCount++;
        }
        return readBytesCount;
    }

    @Override
    public void closeOutbound() throws IOException {
        super.closeOutbound();

        sslEngine.closeOutbound();
        try {
            writeInternal(emptyBuffer);
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    @Override
    public void close() throws IOException {
        socketChannel.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SSLSocketChannelWrapper{");
        sb.append("socketChannel=").append(socketChannel);
        sb.append('}');
        return sb.toString();
    }
}
