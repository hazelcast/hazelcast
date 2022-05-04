package com.hazelcast.tpc.engine;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AsyncServerSocket {

    protected final ILogger logger = Logger.getLogger(this.getClass());
    protected AtomicBoolean closed = new AtomicBoolean(false);

    public final SocketAddress getLocalAddress() {
        try {
            return localAddress();
        } catch (Error e) {
            throw e;
        } catch (Exception e) {
            return null;
        }
    }

    protected abstract SocketAddress localAddress() throws Exception;

    public abstract boolean isReusePort();

    public abstract void setReusePort(boolean reusePort);

    public abstract boolean isReuseAddress();

    public abstract void setReuseAddress(boolean reuseAddress);

    public abstract void setReceiveBufferSize(int size);

    public abstract int getReceiveBufferSize();

    public abstract void bind(SocketAddress socketAddress);

    public void listen(int backlog) {
    }

    public abstract void close();

    public final boolean isClosed() {
        return closed.get();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getLocalAddress() + "]";
    }
}
