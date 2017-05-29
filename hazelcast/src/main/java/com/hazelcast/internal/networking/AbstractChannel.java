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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

/**
 * An abstract {@link Channel} implementation. This class is a pure implementation detail, the fact that it exposes some
 * functionality like access to the socket channel is because the current Channel implementations need the SocketChannel.
 */
public abstract class AbstractChannel implements Channel {

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<AbstractChannel> CLOSED = newUpdater(AbstractChannel.class, "closed");

    protected final SocketChannel socketChannel;
    private final ConcurrentMap<?, ?> attributeMap = new ConcurrentHashMap<Object, Object>();
    private final Set<ChannelCloseListener> closeListeners
            = newSetFromMap(new ConcurrentHashMap<ChannelCloseListener, Boolean>());
    private final boolean clientMode;
    private volatile int closed = FALSE;

    public AbstractChannel(SocketChannel socketChannel, boolean clientMode) {
        this.socketChannel = socketChannel;
        this.clientMode = clientMode;
    }

    @Override
    public boolean isClientMode() {
        return clientMode;
    }

    public ConcurrentMap attributeMap() {
        return attributeMap;
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
    public int read(ByteBuffer dst) throws IOException {
        return socketChannel.read(dst);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return socketChannel.write(src);
    }

    @Override
    public void closeInbound() throws IOException {
    }

    @Override
    public void closeOutbound() throws IOException {
    }

    @Override
    public boolean isClosed() {
        return closed == TRUE;
    }

    @Override
    public void close() throws IOException {
        if (!CLOSED.compareAndSet(this, FALSE, TRUE)) {
            return;
        }

        try {
            socketChannel.close();
        } finally {
            for (ChannelCloseListener closeListener : closeListeners) {
                // it is important we catch exceptions so that other listeners aren't obstructed when
                // one of the listeners is throwing an exception.
                try {
                    closeListener.onClose(this);
                } catch (Exception e) {
                    ILogger logger = Logger.getLogger(getClass());
                    logger.severe(format("Failed to process closeListener [%s] on channel [%s]", closeListener, this), e);
                }
            }
        }
    }

    @Override
    public void addCloseListener(ChannelCloseListener listener) {
        closeListeners.add(checkNotNull(listener, "listener"));
    }
}
