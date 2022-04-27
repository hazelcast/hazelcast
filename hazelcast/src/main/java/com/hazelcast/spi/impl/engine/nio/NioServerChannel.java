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

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.nio.channels.SelectionKey.OP_ACCEPT;

public final class NioServerChannel implements NioSelectedKeyListener {

    public SocketConfig socketConfig;
    public ServerSocketChannel serverSocketChannel;
    public Supplier<NioChannel> channelSupplier;
    public InetSocketAddress address;

    private Selector selector;
    private ILogger logger;
    private NioEventloop eventloop;

    public void configure(NioEventloop eventloop) throws IOException {
        this.eventloop = eventloop;
        this.selector = eventloop.selector;
        this.logger = eventloop.logger;
        this.serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.setOption(SO_RCVBUF, socketConfig.receiveBufferSize);
        System.out.println(eventloop.getName() + " Binding to " + address);
        serverSocketChannel.bind(address);
        serverSocketChannel.configureBlocking(false);
    }

    public void accept() throws Exception {
        serverSocketChannel.register(selector, OP_ACCEPT, this);
        System.out.println(eventloop.getName() + " ServerSocket listening at " + serverSocketChannel.getLocalAddress());
    }

    @Override
    public void handleException(Exception e) {
        logger.severe("NioServerChannel ran into a fatal exception", e);
    }

    @Override
    public void handle(SelectionKey key) throws IOException {
        SocketChannel socketChannel = serverSocketChannel.accept();
        NioChannel channel = channelSupplier.get();

        try {
            channel.handleAccepted(eventloop, socketChannel, socketConfig);
            logger.info("Connection Accepted: " + socketChannel.getLocalAddress());
        } catch (IOException e) {
            channel.handleException(e);
        }
    }
}
