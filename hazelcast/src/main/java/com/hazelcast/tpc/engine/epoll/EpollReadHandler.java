package com.hazelcast.tpc.engine.epoll;

import com.hazelcast.tpc.engine.ReadHandler;

import java.nio.ByteBuffer;

public interface EpollReadHandler extends ReadHandler {

    void init(EpollAsyncSocket asyncSocket);

    void onRead(ByteBuffer receiveBuffer);
}
