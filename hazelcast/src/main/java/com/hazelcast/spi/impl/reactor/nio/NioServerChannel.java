package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.reactor.SocketConfig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Supplier;

import static java.nio.channels.SelectionKey.OP_READ;

public class NioServerChannel {

    public NioReactor reactor;
    public SocketConfig socketConfig;
    public ServerSocketChannel serverSocketChannel;
    public Selector selector;
    public ILogger logger;
    public NioReactorConfig config;
    public Supplier<NioChannel> channelSupplier;
    public InetSocketAddress address;

    public void handleAccept() {
        try {
            SocketChannel socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            reactor.configure(socketChannel.socket(), socketConfig);

            SelectionKey channelSelectionKey = socketChannel.register(selector, OP_READ);

            NioChannel channel = channelSupplier.get();
            channel.reactor = reactor;
           //todo
            // channel.writeThrough = config.writeThrough;
            channel.key = channelSelectionKey;
            channel.receiveBuffer = ByteBuffer.allocateDirect(socketConfig.receiveBufferSize);
            channel.socketChannel = socketChannel;
            //channel.connection = null;
            channel.remoteAddress = socketChannel.getRemoteAddress();
            channel.localAddress = socketChannel.getLocalAddress();
            reactor.registeredChannels.add(channel);

            channelSelectionKey.attach(channel);

            logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
