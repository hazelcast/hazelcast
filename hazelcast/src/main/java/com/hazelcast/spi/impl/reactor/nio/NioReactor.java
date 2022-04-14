package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.spi.impl.reactor.Channel;
import com.hazelcast.spi.impl.reactor.Reactor;

import java.io.IOException;
import java.net.SocketAddress;
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

public final class NioReactor extends Reactor {
    final Selector selector;
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

            SocketChannel socketChannel = SocketChannel.open();

            channel.configure(this, socketChannel, c.socketConfig);

            socketChannel.connect(address);
            socketChannel.configureBlocking(false);

            schedule(() -> {
                try {
                    channel.onConnectionEstablished();
                    registeredChannels.add(channel);
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

}
