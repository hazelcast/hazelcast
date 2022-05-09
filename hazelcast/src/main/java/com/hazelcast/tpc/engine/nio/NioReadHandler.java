package com.hazelcast.tpc.engine.nio;

import com.hazelcast.tpc.engine.ReadHandler;

import java.nio.ByteBuffer;

public abstract class NioReadHandler implements ReadHandler {

    protected NioAsyncSocket asyncSocket;

    public void init(NioAsyncSocket asyncSocket) {
        this.asyncSocket = asyncSocket;
    }

    public abstract void onRead(ByteBuffer buffer);
}
