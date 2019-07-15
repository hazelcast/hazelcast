package com.hazelcast.hazelfast;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;

class SupportConnection {
    SocketChannel channel;

    final ByteArrayPool byteArrayPool;
    final FramePool framePool;

    ByteBuffer receiveBuf;
    Frame receiveFrame;
    long onReadEvents;
    int receiveOffset;
    long readFrames;
    long bytesRead;

    long bytesWritten;
    final ArrayDeque<Frame> pending = new ArrayDeque<>();
    int onWriteEvents;
    int sendOffset;
    Frame sendFrame;
    ByteBuffer sendBuf;

    SupportConnection(boolean objectPoolingEnabled) {
        this.byteArrayPool = new ByteArrayPool(objectPoolingEnabled);
        this.framePool = new FramePool(objectPoolingEnabled);
    }
}
