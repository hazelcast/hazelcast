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

package com.hazelcast.nio;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;

public class DefaultSocketChannelWrapper implements SocketChannelWrapper {
    protected final SocketChannel socketChannel;

    public DefaultSocketChannelWrapper(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public boolean isBlocking() {
        return socketChannel.isBlocking();
    }

    public int validOps() {
        return socketChannel.validOps();
    }

    public Socket socket() {
        return socketChannel.socket();
    }

    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    public boolean isConnectionPending() {
        return socketChannel.isConnectionPending();
    }

    public boolean connect(SocketAddress socketAddress) throws IOException {
        return socketChannel.connect(socketAddress);
    }

    public boolean finishConnect() throws IOException {
        return socketChannel.finishConnect();
    }

    public int read(ByteBuffer byteBuffer) throws IOException {
        return socketChannel.read(byteBuffer);
    }

    public long read(ByteBuffer[] byteBuffers, int offset, int length) throws IOException {
        return socketChannel.read(byteBuffers, offset, length);
    }

    public long read(ByteBuffer[] byteBuffers) throws IOException {
        return socketChannel.read(byteBuffers);
    }

    public int write(ByteBuffer byteBuffer) throws IOException {
        return socketChannel.write(byteBuffer);
    }

    public long write(ByteBuffer[] byteBuffers, int offset, int length) throws IOException {
        return socketChannel.write(byteBuffers, offset, length);
    }

    public long write(ByteBuffer[] byteBuffers) throws IOException {
        return socketChannel.write(byteBuffers);
    }

    public SelectableChannel configureBlocking(boolean b) throws IOException {
        return socketChannel.configureBlocking(b);
    }

    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    public void close() throws IOException {
        socketChannel.close();
    }

    public SelectionKey keyFor(Selector selector) {
        return socketChannel.keyFor(selector);
    }

    public SelectionKey register(Selector selector, int ops, Object attachment) throws ClosedChannelException {
        return socketChannel.register(selector, ops, attachment);
    }
}
