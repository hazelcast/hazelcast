package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.spi.impl.reactor.frame.Frame;

import java.net.SocketAddress;

import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;


public abstract class Channel {
    public Connection connection;
    public SocketAddress remoteAddress;
    public SocketAddress localAddress;

    public final SwCounter packetsWritten = newSwCounter();

    public final SwCounter bytesRead = newSwCounter();

    public final SwCounter  bytesWritten = newSwCounter();

    public final SwCounter  framesRead = newSwCounter();

    public final SwCounter handleWriteCnt = newSwCounter();

    public final SwCounter  readEvents = newSwCounter();

    public abstract void flush();

    public abstract void write(Frame frame);

    public abstract void writeAndFlush(Frame frame);

    /**
     * Should only be called from within the reactor.
     *
     * @param frame
     */
    public abstract void unsafeWriteAndFlush(Frame frame);

    public abstract void close();

    @Override
    public String toString(){
        return getClass().getSimpleName()+"["+localAddress+"->"+remoteAddress+"]";
    }
}
