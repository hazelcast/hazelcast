package com.hazelcast.tpc.engine.nio;

import com.hazelcast.tpc.engine.ReadHandler;
import com.hazelcast.tpc.engine.iobuffer.IOBuffer;

import java.nio.ByteBuffer;

public abstract class NioSyncReadHandler implements ReadHandler {

    protected NioSyncSocket socket;

    public void init(NioSyncSocket socket) {
        this.socket = socket;
    }

    public abstract IOBuffer decode(ByteBuffer buffer);
}