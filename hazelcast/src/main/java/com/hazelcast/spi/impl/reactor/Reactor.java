package com.hazelcast.spi.impl.reactor;


import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public abstract class Reactor extends HazelcastManagedThread {

    protected final ReactorFrontEnd frontend;
    protected final ILogger logger;
    protected final Address thisAddress;
    protected final int port;

    public Reactor(ReactorFrontEnd frontend, Address thisAddress, int port, String name) {
        super(name);
        this.frontend = frontend;
        this.logger = frontend.logger;
        this.thisAddress = thisAddress;
        this.port = port;
    }

    public abstract void wakeup();

    public abstract void enqueue(Request request);

    public abstract Future<Channel> enqueue(SocketAddress address, Connection connection);

    @Override
    protected void executeRun() {
        System.out.println(getName() + " Died: frontend shutting down:" + frontend.shuttingdown);
    }

    public static class ConnectRequest {
        public Connection connection;
        public SocketAddress address;
        public CompletableFuture<Channel> future;
    }
}
