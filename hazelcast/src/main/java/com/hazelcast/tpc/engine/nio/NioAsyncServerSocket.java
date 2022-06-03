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

package com.hazelcast.tpc.engine.nio;

import com.hazelcast.tpc.engine.AsyncServerSocket;
import com.hazelcast.tpc.engine.Eventloop;

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
import static java.nio.channels.SelectionKey.OP_ACCEPT;

public final class NioAsyncServerSocket extends AsyncServerSocket {

    private final ServerSocketChannel serverSocketChannel;
    private final Selector selector;
    private final NioEventloop eventloop;
    private final Eventloop.EventloopThread eventloopThread;

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

    public ServerSocketChannel serverSocketChannel() {
        return serverSocketChannel;
    }

    @Override
    protected SocketAddress getLocalAddress0() throws IOException {
        return serverSocketChannel.getLocalAddress();
    }

    @Override
    public boolean isReusePort() {
        //serverSocketChannel.getOption(StandardSocketOptions.SO_REUSEPORT)
        return false;
    }

    @Override
    public void setReusePort(boolean reusePort) {
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
    public void close() {
        if (closed.compareAndSet(false, true)) {
            System.out.println("Closing  " + this);
            closeResource(serverSocketChannel);
            eventloop.deregisterResource(this);
        }
    }

    @Override
    public void bind(SocketAddress local) {
        try {
            System.out.println(eventloopThread.getName() + " Binding to " + local);
            serverSocketChannel.bind(local);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void accept(Consumer<NioAsyncSocket> consumer) {
        eventloop.execute(() -> {
            serverSocketChannel.register(selector, OP_ACCEPT, new EventloopHandler(consumer));
            System.out.println(eventloopThread.getName() + " ServerSocket listening at " + serverSocketChannel.getLocalAddress());
        });
    }

    private class EventloopHandler implements NioSelectedKeyListener {

        private final Consumer<NioAsyncSocket> consumer;

        private EventloopHandler(Consumer<NioAsyncSocket> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void handleException(Exception e) {
            logger.severe("NioServerChannel ran into a fatal exception", e);
        }

        @Override
        public void handle(SelectionKey key) throws IOException {
            SocketChannel socketChannel = serverSocketChannel.accept();
            NioAsyncSocket socket = new NioAsyncSocket(socketChannel);
            consumer.accept(socket);

            logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
        }
    }
}
