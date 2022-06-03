package com.hazelcast.tpc.engine.nio;

import com.hazelcast.tpc.engine.ReadHandler;
import com.hazelcast.tpc.engine.frame.Frame;

import java.nio.ByteBuffer;

public abstract class NioSyncReadHandler implements ReadHandler {

    protected NioSyncSocket socket;

    public void init(NioSyncSocket socket) {
        this.socket = socket;
    }

    public abstract Frame decode(ByteBuffer buffer);
}