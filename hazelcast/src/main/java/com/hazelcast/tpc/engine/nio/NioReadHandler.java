package com.hazelcast.tpc.engine.nio;

import com.hazelcast.tpc.engine.AsyncSocketReadHandler;

import java.nio.ByteBuffer;

public abstract class NioReadHandler implements AsyncSocketReadHandler {

    protected NioAsyncSocket asyncSocket;

    public void init(NioAsyncSocket asyncSocket){
        this.asyncSocket = asyncSocket;
    }

    public abstract void onRead(ByteBuffer buffer);
}
