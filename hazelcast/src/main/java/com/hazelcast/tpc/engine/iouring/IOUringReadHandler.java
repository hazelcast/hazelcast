package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.tpc.engine.ReadHandler;
import io.netty.buffer.ByteBuf;

public interface IOUringReadHandler extends ReadHandler {

    void init(IOUringAsyncSocket asyncSocket);

    void onRead(ByteBuf receiveBuffer);
}
