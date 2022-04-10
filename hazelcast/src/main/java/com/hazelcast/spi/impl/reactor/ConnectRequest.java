package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.nio.Connection;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

public class ConnectRequest {
    public Connection connection;
    public SocketAddress address;
    public CompletableFuture<Channel> future;
}
