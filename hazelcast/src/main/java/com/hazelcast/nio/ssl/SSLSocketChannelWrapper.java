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
    private final ByteBuffer out;
    private final ByteBuffer cTOs;      // "reliable" write transport
    private final ByteBuffer sTOc;      // "reliable" read transport
    private final SSLEngine sslEngine;
    private SSLEngineResult sslEngineResult;

    public SSLSocketChannelWrapper(SSLContext sslContext, SocketChannel sc, boolean client) throws Exception {
        super(sc);
        sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(client);
        sslEngine.setEnableSessionCreation(true);
        SSLSession session = sslEngine.getSession();
        in = ByteBuffer.allocate(64 * 1024);
        int appBufferMax = session.getApplicationBufferSize();
        int netBufferMax = session.getPacketBufferSize();
        out = ByteBuffer.allocate(appBufferMax);
        cTOs = ByteBuffer.allocate(netBufferMax);
        sTOc = ByteBuffer.allocate(netBufferMax);
        write(out);
        while (sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
            if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                sTOc.clear();
                while (socketChannel.read(sTOc) < 1) {
                    Thread.sleep(50);
                }
                sTOc.flip();
                unwrap(sTOc);
                if (sslEngineResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                    out.clear();
                    write(out);
                }
            } else if (sslEngineResult.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                out.clear();
                write(out);
            } else {
                Thread.sleep(500);
            }
        }
        in.clear();
        in.flip();
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
        return in;
    }

    public int write(ByteBuffer input) throws IOException {
        sslEngineResult = sslEngine.wrap(input, cTOs);
        cTOs.flip();
        int written = socketChannel.write(cTOs);
        if (cTOs.hasRemaining()) {
            cTOs.compact();
        } else {
            cTOs.clear();
        }
        return written;
    }

    public int read(ByteBuffer output) throws IOException {
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
        if (sTOc.hasRemaining()) {
            unwrap(sTOc);
            in.flip();
            limit = Math.min(in.limit(), output.remaining());
            for (int i = 0; i < limit; i++) {
                output.put(in.get());
                readBytesCount++;
            }
            if (sslEngineResult.getStatus() != SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                sTOc.clear();
                sTOc.flip();
                return readBytesCount;
            }
        }
        if (sTOc.hasRemaining()) {
            sTOc.compact();
        } else {
            sTOc.clear();
        }
        if (socketChannel.read(sTOc) == -1) {
            sTOc.clear();
            sTOc.flip();
            return -1;
        }
        sTOc.flip();
        unwrap(sTOc);
        in.flip();
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
            out.clear();
            write(out);
        } catch (Exception ignored) {
        }
        socketChannel.close();
    }

    @Override
    public long read(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
        throw new UnsupportedOperationException();
    }
}