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

package com.hazelcast.nio.tcp;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class DefaultSocketChannelWrapper implements SocketChannelWrapper {

    protected final SocketChannel socketChannel;

    public DefaultSocketChannelWrapper(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    @Override
    public boolean isBlocking() {
        return socketChannel.isBlocking();
    }

    @Override
    public Socket socket() {
        return socketChannel.socket();
    }

    @Override
    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    @Override
    public boolean connect(SocketAddress socketAddress) throws IOException {
        return socketChannel.connect(socketAddress);
    }

    @Override
    public int read(ByteBuffer byteBuffer) throws IOException {
        return socketChannel.read(byteBuffer);
    }

    @Override
    public int write(ByteBuffer byteBuffer) throws IOException {
        return socketChannel.write(byteBuffer);
    }

    @Override
    public SelectableChannel configureBlocking(boolean b) throws IOException {
        return socketChannel.configureBlocking(b);
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public void closeInbound() throws IOException {
    }

    @Override
    public void closeOutbound() throws IOException {
    }

    @Override
    public void close() throws IOException {
        socketChannel.close();
    }

    @Override
    public SelectionKey keyFor(Selector selector) {
        return socketChannel.keyFor(selector);
    }

    @Override
    public SelectionKey register(Selector selector, int ops, Object attachment) throws ClosedChannelException {
        return socketChannel.register(selector, ops, attachment);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DefaultSocketChannelWrapper{");
        sb.append("socketChannel=").append(socketChannel);
        sb.append('}');
        return sb.toString();
    }
}
