package com.hazelcast.tpc.engine.nio;

import com.hazelcast.tpc.engine.ReadHandler;
import com.hazelcast.tpc.engine.frame.Frame;

import java.nio.ByteBuffer;

public abstract class NioSyncReadHandler implements ReadHandler {

    protected NioSyncSocket syncSocket;

    public void init(NioSyncSocket syncSocket) {
        this.syncSocket = syncSocket;
    }

    public abstract Frame decode(ByteBuffer buffer);
}