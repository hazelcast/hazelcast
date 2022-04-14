package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Reactor;
import com.hazelcast.spi.impl.reactor.SocketConfig;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.nio.channels.SelectionKey.OP_ACCEPT;
import static java.nio.channels.SelectionKey.OP_READ;

public final class NioReactor extends Reactor {
    private final Selector selector;
    private final boolean spin;
    private final AtomicBoolean wakeupNeeded = new AtomicBoolean(true);

    public NioReactor(NioReactorConfig config) {
        super(config);
        this.spin = config.spin;
        this.selector = SelectorOptimizer.newSelector(logger);
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == this) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    @Override
    protected void eventLoop() throws Exception {
        while (running) {
            runTasks();

            boolean moreWork = scheduler.tick();

            flushDirtyChannels();

            int keyCount;
            if (spin || moreWork) {
                keyCount = selector.selectNow();
            } else {
                wakeupNeeded.set(true);
                if (publicRunQueue.isEmpty()) {
                    keyCount = selector.select();
                } else {
                    keyCount = selector.selectNow();
                }
                wakeupNeeded.set(false);
            }

            if (keyCount > 0) {
                handleSelectedKeys();
            }
        }
    }

    private void handleSelectedKeys() {
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
            SelectionKey key = it.next();
            it.remove();

            if (key.isValid() && key.isAcceptable()) {
                ((NioServerChannel) key.attachment()).handleAccept();
            }

            if (key.isValid() && key.isReadable()) {
                ((NioChannel) key.attachment()).handleRead();
            }

            if (key.isValid() && key.isWritable()) {
                ((NioChannel) key.attachment()).handleWrite();
            }

            if (!key.isValid()) {
                key.cancel();
            }
        }
    }

    public void accept(NioServerChannel serverChannel) throws IOException {
        serverChannel.reactor = this;
        serverChannel.selector = selector;
        serverChannel.logger = logger;

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.setOption(SO_RCVBUF, serverChannel.socketConfig.receiveBufferSize);
        System.out.println(getName() + " Binding to " + serverChannel.address);
        serverSocketChannel.bind(serverChannel.address);
        serverSocketChannel.configureBlocking(false);
        schedule(() -> {
            SelectionKey key = serverSocketChannel.register(selector, OP_ACCEPT);
            System.out.println(getName() + " ServerSocket listening at " + serverSocketChannel.getLocalAddress());
            serverChannel.serverSocketChannel = serverSocketChannel;
            key.attach(serverChannel);
        });
    }

    @Override
    public Future<Channel> connect(Channel c, SocketAddress address) {
        NioChannel channel = (NioChannel) c;

        CompletableFuture<Channel> future = new CompletableFuture();
        try {

            System.out.println("ConnectRequest address:" + address);

            SocketConfig socketConfig = c.config;

            SocketChannel socketChannel = SocketChannel.open();
            Socket socket = socketChannel.socket();
            configure(socket, socketConfig);

            socketChannel.connect(address);
            socketChannel.configureBlocking(false);

            schedule(() -> {
                try {
                    SelectionKey key = socketChannel.register(selector, OP_READ);
                    channel.reactor = NioReactor.this;
                    channel.key = key;
                    channel.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);
                    channel.socketChannel = socketChannel;
                    channel.remoteAddress = socketChannel.getRemoteAddress();
                    channel.localAddress = socketChannel.getLocalAddress();
                    registeredChannels.add(channel);
                    key.attach(channel);

                    logger.info("Socket listening at " + address);
                    future.complete(channel);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    protected void configure(Socket socket, SocketConfig socketConfig) throws SocketException {
        socket.setTcpNoDelay(socketConfig.tcpNoDelay);
        socket.setSendBufferSize(socketConfig.sendBufferSize);
        socket.setReceiveBufferSize(socketConfig.receiveBufferSize);

        String id = socket.getLocalAddress() + "->" + socket.getRemoteSocketAddress();
        System.out.println(getName() + " " + id + " tcpNoDelay: " + socket.getTcpNoDelay());
        System.out.println(getName() + " " + id + " receiveBufferSize: " + socket.getReceiveBufferSize());
        System.out.println(getName() + " " + id + " sendBufferSize: " + socket.getSendBufferSize());
    }
}
