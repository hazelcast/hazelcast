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

package com.hazelcast.internal.tpc.nio;

import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocket;
import com.hazelcast.internal.tpc.Eventloop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.ReflectionUtil.findStaticFieldValue;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.nio.channels.SelectionKey.OP_ACCEPT;

/**
 * Nio implementation of the {@link AsyncServerSocket}.
 */
public final class NioAsyncServerSocket extends AsyncServerSocket {
    private static final AtomicBoolean SO_REUSE_PORT_PRINTED = new AtomicBoolean();

    // This option is available since Java 9, so we need to use reflection.
    private static final SocketOption<Boolean> SO_REUSEPORT = findStaticFieldValue(StandardSocketOptions.class, "SO_REUSEPORT");

    private final ServerSocketChannel serverSocketChannel;
    private final Selector selector;
    private final NioEventloop eventloop;
    private final Thread eventloopThread;

    /**
     * Opens a TCP/IP (stream) based IPv4 NioAsyncServerSocket.
     * <p/>
     * To prevent coupling to Nio, it is better to use the {@link Eventloop#openTcpAsyncServerSocket()}.
     *
     * @param eventloop the eventloop the opened socket will be processed by.
     * @return the opened NioAsyncServerSocket.
     */
    public static NioAsyncServerSocket openTcpServerSocket(NioEventloop eventloop) {
        return new NioAsyncServerSocket(eventloop);
    }

    private NioAsyncServerSocket(NioEventloop eventloop) {
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            this.eventloop = eventloop;
            this.eventloopThread = eventloop.eventloopThread();
            this.selector = eventloop.selector;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public NioEventloop getEventloop() {
        return eventloop;
    }

    @Override
    protected SocketAddress getLocalAddress0() throws IOException {
        return serverSocketChannel.getLocalAddress();
    }

    @Override
    public int getLocalPort() {
        return serverSocketChannel.socket().getLocalPort();
    }

    @Override
    public boolean isReusePort() {
        if (SO_REUSEPORT == null) {
            return false;
        }

        try {
            return serverSocketChannel.getOption(SO_REUSEPORT);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReusePort(boolean reusePort) {
        if (SO_REUSEPORT == null) {
            if (SO_REUSE_PORT_PRINTED.compareAndSet(false, true)) {
                logger.warning("Ignoring NioAsyncServerSocket.reusePort." +
                        "Please upgrade to Java 9+ to enable the SO_REUSEPORT option.");
            }
        } else {
            try {
                serverSocketChannel.setOption(SO_REUSEPORT, reusePort);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public boolean isReuseAddress() {
        try {
            return serverSocketChannel.getOption(SO_REUSEADDR);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReuseAddress(boolean reuseAddress) {
        try {
            serverSocketChannel.setOption(SO_REUSEADDR, reuseAddress);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setReceiveBufferSize(int size) {
        try {
            serverSocketChannel.setOption(SO_RCVBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getReceiveBufferSize() {
        try {
            return serverSocketChannel.getOption(SO_RCVBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void close0() throws IOException {
        // First we need to obtain the key, because as soon as the
        // serverSocketChannel is closed, the key is deregistered.

        // Can be called on any thread, so we need to make use of the
        // synchronization of the serverSocketChannel to obtain the key
        SelectionKey key = serverSocketChannel.keyFor(selector);

        closeQuietly(serverSocketChannel);

        if (key != null) {
            key.cancel();
        }
    }

    @Override
    public void bind(SocketAddress localAddress, int backlog) {
        checkNotNull(localAddress, "localAddress");
        checkNotNegative(backlog, "backlog");

        try {
            if (logger.isInfoEnabled()) {
                logger.info(eventloopThread.getName() + " Binding to " + localAddress);
            }
            serverSocketChannel.bind(localAddress, backlog);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind to " + localAddress, e);
        } catch (AlreadyBoundException | UnsupportedAddressTypeException | SecurityException e) {
            throw new UncheckedIOException(new IOException("Failed to bind to " + localAddress, e));
        }
    }

    @Override
    public CompletableFuture<Void> accept(Consumer<AsyncSocket> consumer) {
        checkNotNull(consumer, "consumer");

        CompletableFuture<Void> future = new CompletableFuture<>();

        Runnable acceptTask = () -> {
            try {
                serverSocketChannel.register(selector, OP_ACCEPT, new AcceptHandler(consumer));

                if (logger.isInfoEnabled()) {
                    logger.info(getLocalAddress() + " started accepting");
                }

                future.complete(null);
            } catch (RuntimeException | IOException e) {
                future.completeExceptionally(e);
            }
        };

        if (!eventloop.offer(acceptTask)) {
            future.completeExceptionally(new RuntimeException("Failed to offer accept task"));
        }

        return future;
    }

    private final class AcceptHandler implements SelectionKeyListener {

        private final Consumer<AsyncSocket> consumer;

        private AcceptHandler(Consumer<AsyncSocket> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void close(String reason, Exception cause) {
            NioAsyncServerSocket.this.close(reason, cause);
        }

        @Override
        public void handle(SelectionKey key) throws IOException {
            if (!key.isValid()) {
                throw new CancelledKeyException();
            }

            SocketChannel socketChannel = serverSocketChannel.accept();
            NioAsyncSocket socket = new NioAsyncSocket(socketChannel);

            accepted.inc();
            consumer.accept(socket);

            if (logger.isInfoEnabled()) {
                logger.info(NioAsyncServerSocket.this + " accepted: " + socketChannel.getRemoteAddress()
                        + "->" + socketChannel.getLocalAddress());
            }
        }
    }
}
