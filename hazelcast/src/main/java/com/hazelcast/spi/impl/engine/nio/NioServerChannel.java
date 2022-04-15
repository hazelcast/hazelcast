package com.hazelcast.spi.impl.engine.nio;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.engine.SocketConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Supplier;

public class NioServerChannel {

    public NioReactor reactor;
    public SocketConfig socketConfig;
    public ServerSocketChannel serverSocketChannel;
    public Selector selector;
    public ILogger logger;
    public Supplier<NioChannel> channelSupplier;
    public InetSocketAddress address;

    public void handleAccept() {
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
