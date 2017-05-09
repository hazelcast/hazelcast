/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking;

import com.hazelcast.internal.networking.nio.NioChannelReader;
import com.hazelcast.internal.networking.nio.NioChannelWriter;
import com.hazelcast.nio.OutboundFrame;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NioChannel implements Channel {

    protected final SocketChannel socketChannel;
    private NioChannelReader reader;
    private NioChannelWriter writer;
    private final ConcurrentMap<?, ?> attributeMap = new ConcurrentHashMap<Object, Object>();

    public NioChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public NioChannelReader getReader() {
        return reader;
    }

    public NioChannelWriter getWriter() {
        return writer;
    }

    @Override
    public ConcurrentMap attributeMap() {
        return attributeMap;
    }

    public void setReader(NioChannelReader reader) {
        this.reader = reader;
    }

    public void setWriter(NioChannelWriter writer) {
        this.writer = writer;
    }

    @Override
    public Socket socket() {
        return socketChannel.socket();
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        Socket socket = socket();
        return socket == null ? null : socket.getRemoteSocketAddress();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        Socket socket = socket();
        return socket == null ? null : socket.getLocalSocketAddress();
    }

    @Override
    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return socketChannel.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return socketChannel.write(src);
    }

    @Override
    public SelectableChannel configureBlocking(boolean block) throws IOException {
        return socketChannel.configureBlocking(block);
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
    public String toString() {
        return "NioChannel{channel=" + socketChannel + '}';
    }

    @Override
    public void addCloseListener(CloseListener closeListener) {

    }

    @Override
    public long lastWriteTimeMillis() {
        return writer.lastWriteTimeMillis();
    }

    @Override
    public long lastReadTimeMillis() {
        return reader.lastReadTimeMillis();
    }

    @Override
    public boolean write(OutboundFrame frame) {
        if (!socketChannel.isOpen()) {
            return false;
        }
        writer.write(frame);
        return true;
    }
}
