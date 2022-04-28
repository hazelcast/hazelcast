package io.netty.incubator.channel.uring;

import com.hazelcast.spi.impl.engine.ReadHandler;
import io.netty.buffer.ByteBuf;

public interface IOUringReadHandler extends ReadHandler {

    void init(IOUringAsyncSocket asyncSocket);

    void onRead(ByteBuf receiveBuffer);
}
