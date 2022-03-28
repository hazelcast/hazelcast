package com.hazelcast.spi.impl.reactor;

import java.nio.ByteBuffer;

public abstract class Channel {

    public abstract void flush();

    public abstract void write(ByteBuffer buffer);

    public abstract void writeAndFlush(ByteBuffer buffer);
}
