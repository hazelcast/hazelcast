/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.tpc.AcceptRequest;

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
import static com.hazelcast.internal.tpc.util.ExceptionUtil.sneakyThrow;
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
    private final NioReactor reactor;
    private final Thread eventloopThread;
    private final SelectionKey key;
    private Consumer<AcceptRequest> consumer;

    NioAsyncServerSocket(NioReactor reactor) {
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            this.reactor = reactor;
            this.eventloopThread = reactor.eventloopThread();
            this.selector = reactor.selector;
            this.key = serverSocketChannel.register(selector, 0, new Handler());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public NioReactor getReactor() {
        return reactor;
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
                logger.warning("Ignoring NioAsyncServerSocket.reusePort."
                        + "Please upgrade to Java 9+ to enable the SO_REUSEPORT option.");
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
    public void accept(Consumer<AcceptRequest> consumer) {
        checkNotNull(consumer, "consumer");

        if (Thread.currentThread() == eventloopThread) {
            accept0(consumer);
        } else {
            CompletableFuture<Void> future = new CompletableFuture<>();
            reactor.execute(() -> {
                try {
                    accept0(consumer);

                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    throw sneakyThrow(t);
                }
            });

            future.join();
        }
    }

    private void accept0(Consumer<AcceptRequest> consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException(this + " already is accepting");
        }

        this.consumer = consumer;

        key.interestOps(key.interestOps() | OP_ACCEPT);

        if (logger.isInfoEnabled()) {
            logger.info(getLocalAddress() + " started accepting");
        }
    }

    private final class Handler implements NioHandler {

        @Override
        public void close(String reason, Throwable cause) {
            NioAsyncServerSocket.this.close(reason, cause);
        }

        @Override
        public void handle() throws IOException {
            if (!key.isValid()) {
                throw new CancelledKeyException();
            }

            SocketChannel socketChannel = serverSocketChannel.accept();
            NioAcceptRequest openRequest = new NioAcceptRequest(socketChannel);

            accepted.inc();
            consumer.accept(openRequest);

            if (logger.isInfoEnabled()) {
                logger.info(NioAsyncServerSocket.this + " accepted: " + socketChannel.getRemoteAddress()
                        + "->" + socketChannel.getLocalAddress());
            }
        }
    }
}
