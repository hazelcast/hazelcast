package com.hazelcast.spi.impl.reactor.nio;

import com.hazelcast.spi.impl.reactor.SocketConfig;

import java.nio.channels.ServerSocketChannel;

public class NioServerChannel {
    public SocketConfig socketConfig;
    public ServerSocketChannel serverSocketChannel;
}
