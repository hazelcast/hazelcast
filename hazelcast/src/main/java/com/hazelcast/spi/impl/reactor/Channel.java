package com.hazelcast.spi.impl.reactor;

import com.hazelcast.internal.nio.Connection;

import java.net.SocketAddress;


public abstract class Channel {
    public Connection connection;

    public SocketAddress remoteAddress;
    public SocketAddress localAddress;

    public long packetsWritten;
    public long prevPacketsWritten;

    public long bytesRead;
    public long prevBytesRead;

    public long bytesWritten;
    public long prevBytesWritten;

    public long framesRead;
    public long prevPacketsRead ;

    public long handleOutboundCalls;
    public long prevHandleOutboundCalls;

    public long readEvents;
    public long prevReadEvents;

    public int bytesWrittenConfirmed;
    public int prevBytesWrittenConfirmed;

    public abstract void flush();

    public abstract void write(Frame frame);

    public abstract void writeAndFlush(Frame frame);
}
