package io.netty.channel.epoll;

import com.hazelcast.spi.impl.reactor.SocketConfig;

import java.nio.channels.ServerSocketChannel;

public class EpollServerChannel {
    public SocketConfig socketConfig;
    public LinuxSocket socket;
}
