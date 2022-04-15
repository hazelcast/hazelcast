package com.hazelcast.spi.impl.engine.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.engine.SocketConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Supplier;

public class NioServerChannel implements NioSelectionListener {

    public NioReactor reactor;
    public SocketConfig socketConfig;
    public ServerSocketChannel serverSocketChannel;
    public Selector selector;
    public ILogger logger;
    public Supplier<NioChannel> channelSupplier;
    public InetSocketAddress address;

    @Override
    public void handle(SelectionKey key) {
        try {
            SocketChannel socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);

            NioChannel channel = channelSupplier.get();
            channel.configure(reactor, socketChannel, socketConfig);
            channel.onConnectionEstablished();
            reactor.registeredChannels.add(channel);
            logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
