package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.tpc.engine.AsyncSocketReadHandler;
import io.netty.buffer.ByteBuf;

public interface IOUringReadHandler extends AsyncSocketReadHandler {

    void init(IOUringAsyncSocket asyncSocket);

    void onRead(ByteBuf receiveBuffer);
}
