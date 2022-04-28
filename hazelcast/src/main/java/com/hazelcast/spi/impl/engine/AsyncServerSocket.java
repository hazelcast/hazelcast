package com.hazelcast.spi.impl.engine;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AsyncServerSocket {

    protected final ILogger logger = Logger.getLogger(this.getClass());
    protected SocketAddress localAddress;
    protected AtomicBoolean closed = new AtomicBoolean(false);

    public final SocketAddress getLocalAddress() {
        return localAddress;
    }

    public abstract boolean isReusePort();

    public abstract void setReusePort(boolean reusePort);

    public abstract boolean isReuseAddress();

    public abstract void setReuseAddress(boolean reuseAddress);

    public abstract void setReceiveBufferSize(int size);

    public abstract int getReceiveBufferSize();

    public abstract void bind(SocketAddress socketAddress);

    public void listen(int backlog){
    }

    public abstract void close();

    public final boolean isClosed() {
        return closed.get();
    }
}
