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

import com.hazelcast.internal.tpc.AcceptRequest;
import com.hazelcast.internal.tpc.AsyncServerSocket;
import com.hazelcast.internal.tpc.AsyncSocketOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnsupportedAddressTypeException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpc.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static java.nio.channels.SelectionKey.OP_ACCEPT;

/**
 * Nio implementation of the {@link AsyncServerSocket}.
 */
public final class NioAsyncServerSocket extends AsyncServerSocket {

    private final ServerSocketChannel serverSocketChannel;
    private final NioReactor reactor;
    private final Thread eventloopThread;
    private final SelectionKey key;
    private final NioAsyncServerSocketOptions options;
    private final Consumer<AcceptRequest> consumer;
    // only accessed from eventloop thread
    private boolean started;

    NioAsyncServerSocket(NioAsyncServerSocketBuilder builder) {
        try {
            this.reactor = builder.reactor;
            this.consumer = builder.acceptConsumer;
            this.options = builder.options;
            this.eventloopThread = reactor.eventloopThread();
            this.serverSocketChannel = builder.serverSocketChannel;
            this.key = register();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private SelectionKey register() throws IOException {
        if (Thread.currentThread() == eventloopThread) {
            return serverSocketChannel.register(reactor.selector, 0, new Handler());
        } else {
            CompletableFuture<SelectionKey> future = new CompletableFuture<>();
            reactor.execute(() -> {
                try {
                    future.complete(serverSocketChannel.register(reactor.selector, 0, new Handler()));
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    throw sneakyThrow(t);
                }
            });

            return future.join();
        }
    }

    @Override
    public AsyncSocketOptions options() {
        return options;
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
    protected void close0() throws IOException {
        closeQuietly(serverSocketChannel);

        key.cancel();
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
    public void start() {
        if (Thread.currentThread() == eventloopThread) {
            start0();
        } else {
            CompletableFuture<Void> future = new CompletableFuture<>();
            reactor.execute(() -> {
                try {
                    start0();
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                    throw sneakyThrow(t);
                }
            });

            future.join();
        }
    }

    private void start0() {
        if (started) {
            throw new IllegalStateException(this + " is already started");
        }
        started = true;

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
            accepted.inc();
            if (logger.isInfoEnabled()) {
                logger.info(NioAsyncServerSocket.this + " accepted: " + socketChannel.getRemoteAddress()
                        + "->" + socketChannel.getLocalAddress());
            }

            // todo: we need to think about rejection.
            consumer.accept(new NioAcceptRequest(socketChannel));
        }
    }
}
