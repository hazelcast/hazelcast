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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelCloseListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.nio.IOUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.newSetFromMap;

/**
 * An abstract {@link Channel} implementation. This class is a pure implementation
 * detail, the fact that it exposes some functionality like access to the socket
 * channel is because the current Channel implementations need the SocketChannel.
 */
public abstract class AbstractChannel implements Channel {

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    private static final AtomicIntegerFieldUpdater<AbstractChannel> CLOSED
            = AtomicIntegerFieldUpdater.newUpdater(AbstractChannel.class, "closed");
    private static final AtomicReferenceFieldUpdater<AbstractChannel, SocketAddress> LOCAL_ADDRESS
            = AtomicReferenceFieldUpdater.newUpdater(AbstractChannel.class, SocketAddress.class, "localAddress");
    private static final AtomicReferenceFieldUpdater<AbstractChannel, SocketAddress> REMOTE_ADDRESS
            = AtomicReferenceFieldUpdater.newUpdater(AbstractChannel.class, SocketAddress.class, "remoteAddress");

    protected final SocketChannel socketChannel;
    protected final ILogger logger;

    private final ConcurrentMap<?, ?> attributeMap = new ConcurrentHashMap<Object, Object>();
    private final Set<ChannelCloseListener> closeListeners
            = newSetFromMap(new ConcurrentHashMap<ChannelCloseListener, Boolean>());
    private final boolean clientMode;
    @SuppressWarnings("FieldCanBeLocal")
    private volatile SocketAddress remoteAddress;
    @SuppressWarnings("FieldCanBeLocal")
    private volatile SocketAddress localAddress;
    @SuppressWarnings("FieldCanBeLocal")
    private volatile int closed = FALSE;

    public AbstractChannel(SocketChannel socketChannel, boolean clientMode) {
        this.socketChannel = socketChannel;
        this.clientMode = clientMode;
        this.logger = Logger.getLogger(getClass());
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
    public SocketAddress remoteSocketAddress() {
        if (remoteAddress == null) {
            Socket socket = socket();
            if (socket != null) {
                REMOTE_ADDRESS.compareAndSet(this, null, socket.getRemoteSocketAddress());
            }
        }
        return remoteAddress;
    }

    @Override
    public SocketAddress localSocketAddress() {
        if (localAddress == null) {
            Socket socket = socket();
            if (socket != null) {
                LOCAL_ADDRESS.compareAndSet(this, null, socket().getLocalSocketAddress());
            }
        }
        return localAddress;
    }

    @Override
    public void connect(InetSocketAddress address, int timeoutMillis) throws IOException {
        try {
            if (!clientMode) {
                throw new IllegalStateException("Can't call connect on a Channel that isn't in clientMode");
            }

            checkNotNull(address, "address");
            checkNotNegative(timeoutMillis, "timeoutMillis can't be negative");

            // since the connect method is blocking, we need to configure blocking.
            socketChannel.configureBlocking(true);

            if (timeoutMillis > 0) {
                socketChannel.socket().connect(address, timeoutMillis);
            } else {
                socketChannel.connect(address);
            }

            if (logger.isFinestEnabled()) {
                logger.finest("Successfully connected to: " + address + " using socket " + socketChannel.socket());
            }
        } catch (RuntimeException e) {
            IOUtil.closeResource(this);
            throw e;
        } catch (IOException e) {
            IOUtil.closeResource(this);
            IOException newEx = new IOException(e.getMessage() + " to address " + address);
            newEx.setStackTrace(e.getStackTrace());
            throw newEx;
        }
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

        close0();
    }

    /**
     * Template method that is called when the Channel is closed.
     *
     * It will be called only once.
     */
    protected void close0() throws IOException {
    }

    @Override
    public void addCloseListener(ChannelCloseListener listener) {
        closeListeners.add(checkNotNull(listener, "listener"));
    }

    protected final void notifyCloseListeners() {
        for (ChannelCloseListener closeListener : closeListeners) {
            // it is important we catch exceptions so that other listeners aren't obstructed when
            // one of the listeners is throwing an exception
            try {
                closeListener.onClose(AbstractChannel.this);
            } catch (Exception e) {
                logger.severe(format("Failed to process closeListener [%s] on channel [%s]", closeListener, this), e);
            }
        }
    }
}
