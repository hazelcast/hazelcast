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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.net.StandardSocketOptions.SO_REUSEPORT;
import static java.nio.channels.SelectionKey.OP_ACCEPT;

/**
 * Nio version of the {@link AsyncServerSocket}.
 */
public final class NioAsyncServerSocket extends AsyncServerSocket {

    private final ServerSocketChannel serverSocketChannel;
    private final Selector selector;
    private final NioEventloop eventloop;
    private final Thread eventloopThread;

    public static NioAsyncServerSocket open(NioEventloop eventloop) {
        return new NioAsyncServerSocket(eventloop);
    }

    private NioAsyncServerSocket(NioEventloop eventloop) {
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            this.eventloop = eventloop;
            this.eventloopThread = eventloop.eventloopThread();
            this.selector = eventloop.selector;
            if (!eventloop.registerResource(this)) {
                close();
                throw new IllegalStateException("EventLoop is not running");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public NioEventloop eventloop() {
        return eventloop;
    }

    /**
     * Returns the underlying {@link ServerSocketChannel}.
     *
     * @return the ServerSocketChannel.
     */
    public ServerSocketChannel serverSocketChannel() {
        return serverSocketChannel;
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
//        try {
//            return serverSocketChannel.getOption(SO_REUSEPORT);
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
        return false;
    }

    @Override
    public void setReusePort(boolean reusePort) {
//        try {
//            serverSocketChannel.setOption(SO_REUSEPORT, reusePort);
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
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
    public void reuseAddress(boolean reuseAddress) {
        try {
            serverSocketChannel.setOption(SO_REUSEADDR, reuseAddress);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void receiveBufferSize(int size) {
        try {
            serverSocketChannel.setOption(SO_RCVBUF, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int receiveBufferSize() {
        try {
            return serverSocketChannel.getOption(SO_RCVBUF);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void doClose() {
        closeResource(serverSocketChannel);
        eventloop.deregisterResource(this);
    }


    @Override
    public void listen(int backlog) {
        // ignore; not needed for serverSocketChannel
    }

    @Override
    public void bind(SocketAddress socketAddress) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info(eventloopThread.getName() + " Binding to " + socketAddress);
            }
            serverSocketChannel.bind(socketAddress);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to bind to " + socketAddress, e);
        }
    }

    public void accept(Consumer<NioAsyncSocket> consumer) {
        eventloop.offer(() -> {
            try {
                serverSocketChannel.register(selector, OP_ACCEPT, new EventloopHandler(consumer));

                if (logger.isInfoEnabled()) {
                    logger.info(eventloopThread.getName() + " ServerSocket listening at "
                            + serverSocketChannel.getLocalAddress());
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to accept", e);
            }
        });
    }

    private class EventloopHandler implements NioSelectedKeyListener {

        private final Consumer<NioAsyncSocket> consumer;

        private EventloopHandler(Consumer<NioAsyncSocket> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void handleException(Exception e) {
            logger.severe(this + " ran into a fatal exception", e);
        }

        @Override
        public void handle(SelectionKey key) throws IOException {
            SocketChannel socketChannel = serverSocketChannel.accept();
            NioAsyncSocket socket = new NioAsyncSocket(socketChannel);
            consumer.accept(socket);

            if (logger.isInfoEnabled()) {
                logger.info("Connection Accepted: " + socketChannel.getRemoteAddress()
                        + "->" + socketChannel.getLocalAddress());
            }
        }
    }
}
