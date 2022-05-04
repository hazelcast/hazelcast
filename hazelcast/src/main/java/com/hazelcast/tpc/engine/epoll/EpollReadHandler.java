package com.hazelcast.tpc.engine.epoll;

import com.hazelcast.tpc.engine.AsyncSocketReadHandler;

import java.nio.ByteBuffer;

public interface EpollReadHandler extends AsyncSocketReadHandler {

    void init(EpollAsyncSocket asyncSocket);

    void onRead(ByteBuffer receiveBuffer);
}
