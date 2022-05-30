package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.tpc.engine.ReadHandler;
import io.netty.buffer.ByteBuf;

public abstract class IOUringReadHandler implements ReadHandler {

    protected IOUringAsyncSocket asyncSocket;

    public void init(IOUringAsyncSocket asyncSocket){
        this.asyncSocket = asyncSocket;
    }

    public abstract void onRead(ByteBuf receiveBuffer);
}
