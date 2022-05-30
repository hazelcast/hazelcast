package com.hazelcast.tpc.engine.epoll;

import com.hazelcast.tpc.engine.ReadHandler;

import java.nio.ByteBuffer;

public abstract class EpollReadHandler implements ReadHandler {

    private EpollAsyncSocket asyncSocket;

    public void init(EpollAsyncSocket asyncSocket){
        this.asyncSocket = asyncSocket;
    }

    public abstract void onRead(ByteBuffer receiveBuffer);
}
