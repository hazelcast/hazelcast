package com.hazelcast.spi.impl.engine.nio;

import com.hazelcast.spi.impl.engine.ReadHandler;

import java.nio.ByteBuffer;

public abstract class NioReadHandler implements ReadHandler {

    protected NioAsyncSocket asyncSocket;

    public void init(NioAsyncSocket asyncSocket){
        this.asyncSocket = asyncSocket;
    }

    public abstract void onRead(ByteBuffer buffer);

}
