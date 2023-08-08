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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.Option;
import com.hazelcast.internal.tpcengine.net.AbstractAsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;

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

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static java.nio.channels.SelectionKey.OP_ACCEPT;

/**
 * Nio implementation of the {@link AsyncServerSocket}.
 */
public final class NioAsyncServerSocket extends AsyncServerSocket {

    private final ServerSocketChannel serverSocketChannel;
    private final SelectionKey key;

    NioAsyncServerSocket(Builder builder) {
        super(builder);
        try {
            this.serverSocketChannel = builder.serverSocketChannel;
            this.key = serverSocketChannel.register(builder.selector, 0, new Handler());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        super.close0();

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
            throw newUncheckedIOException("Failed to bind to " + localAddress, e);
        }
    }

    @Override
    protected void start0() {
        key.interestOps(key.interestOps() | OP_ACCEPT);
    }

    @SuppressWarnings("java:S1135")
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
            metrics.incAccepted();
            if (logger.isInfoEnabled()) {
                logger.info(NioAsyncServerSocket.this
                        + " accepted: " + socketChannel.getRemoteAddress()
                        + "->" + socketChannel.getLocalAddress());
            }

            AcceptRequest acceptRequest = new AcceptRequest(socketChannel);
            try {
                acceptFn.accept(acceptRequest);
            } catch (Throwable t) {
                closeQuietly(acceptRequest);
                throw sneakyThrow(t);
            }
        }
    }

    /**
     * The Options for the {@link NioAsyncServerSocket}.
     */
    public static class NioOptions implements AsyncSocket.Options {

        private final ServerSocketChannel serverSocketChannel;

        NioOptions(ServerSocketChannel serverSocketChannel) {
            this.serverSocketChannel = serverSocketChannel;
        }

        private static SocketOption toSocketOption(Option option) {
            if (SO_RCVBUF.equals(option)) {
                return StandardSocketOptions.SO_RCVBUF;
            } else if (SO_REUSEADDR.equals(option)) {
                return StandardSocketOptions.SO_REUSEADDR;
            } else if (SO_REUSEPORT.equals(option)) {
                return StandardSocketOptions.SO_REUSEPORT;
            } else {
                return null;
            }
        }

        @Override
        public boolean isSupported(Option option) {
            checkNotNull(option, "option");

            SocketOption socketOption = toSocketOption(option);
            return isSupported(socketOption);
        }

        private boolean isSupported(SocketOption socketOption) {
            return socketOption != null && serverSocketChannel.supportedOptions().contains(socketOption);
        }

        @Override
        public <T> boolean set(Option<T> option, T value) {
            checkNotNull(option, "option");
            checkNotNull(value, "value");

            try {
                SocketOption socketOption = toSocketOption(option);
                if (isSupported(socketOption)) {
                    serverSocketChannel.setOption(socketOption, value);
                    return true;
                } else {
                    return false;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to set " + option.name()
                        + " with value [" + value + "]", e);
            }
        }

        @Override
        public <T> T get(Option<T> option) {
            checkNotNull(option, "option");

            try {
                SocketOption socketOption = toSocketOption(option);
                if (isSupported(socketOption)) {
                    return (T) serverSocketChannel.getOption(socketOption);
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to get option " + option.name(), e);
            }
        }
    }

    /**
     * An {@link AsyncServerSocket} builder.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends AsyncServerSocket.Builder {

        public ServerSocketChannel serverSocketChannel;
        public Selector selector;

        Builder() {
            // todo: should move into conclude0
            try {
                this.serverSocketChannel = ServerSocketChannel.open();
                serverSocketChannel.configureBlocking(false);
                this.options = new NioOptions(serverSocketChannel);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNull(selector, "selector");
            checkNotNull(serverSocketChannel, "serverSocketChannel");
        }

        @Override
        public AsyncServerSocket construct() {
            if (Thread.currentThread() == reactor.eventloopThread()) {
                return new NioAsyncServerSocket(this);
            } else {
                CompletableFuture<NioAsyncServerSocket> future = reactor.submit(
                        () -> new NioAsyncServerSocket(Builder.this));
                return future.join();
            }
        }
    }

    public static class AcceptRequest implements AbstractAsyncSocket.AcceptRequest {

        final SocketChannel socketChannel;

        AcceptRequest(SocketChannel socketChannel) {
            this.socketChannel = checkNotNull(socketChannel, "socketChannel");
        }

        @Override
        public void close() throws Exception {
            socketChannel.close();
        }
    }
}
