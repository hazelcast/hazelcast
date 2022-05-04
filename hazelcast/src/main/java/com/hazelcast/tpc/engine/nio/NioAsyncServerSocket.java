package com.hazelcast.tpc.engine.nio;

import com.hazelcast.tpc.engine.AsyncServerSocket;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.nio.channels.SelectionKey.OP_ACCEPT;

public final class NioAsyncServerSocket extends AsyncServerSocket {

    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    private NioEventloop eventloop;

    public static NioAsyncServerSocket open(NioEventloop eventloop) {
        return new NioAsyncServerSocket(eventloop);
    }

    private NioAsyncServerSocket(NioEventloop eventloop) {
        try {
            this.eventloop = eventloop;
            this.eventloop.registerServerSocket(this);
            this.selector = eventloop.selector;
            this.serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ServerSocketChannel serverSocketChannel() {
        return serverSocketChannel;
    }

    @Override
    public SocketAddress getLocalAddress() {
        try {
            return serverSocketChannel.getLocalAddress();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    public void close() {
        if (closed.compareAndSet(false, true)) {
            eventloop.deregisterSocket(this);

            try {
                serverSocketChannel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void bind(SocketAddress local) {
        try {
            System.out.println(eventloop.getName() + " Binding to " + local);
            serverSocketChannel.bind(local);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void accept(Consumer<NioAsyncSocket> consumer) {
        eventloop.execute(() -> {
            serverSocketChannel.register(selector, OP_ACCEPT, new AcceptHandler(consumer));
            System.out.println(eventloop.getName() + " ServerSocket listening at " + serverSocketChannel.getLocalAddress());
        });
    }

    private class AcceptHandler implements NioSelectedKeyListener {

        private final Consumer<NioAsyncSocket> consumer;

        private AcceptHandler(Consumer<NioAsyncSocket> consumer) {
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
