package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.frame.FrameAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.incubator.channel.uring.IOUringAsyncSocket;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.spi.impl.engine.frame.Frame.FLAG_OP_RESPONSE;

public class RequestIOUringChannel extends IOUringAsyncSocket {
    private Frame inboundFrame;
    public FrameAllocator requestFrameAllocator;
    public FrameAllocator remoteResponseFrameAllocator;
    public RequestService requestService;
    public OpScheduler opScheduler;

    @Override
    public void onRead(ByteBuf receiveBuffer) {
        Frame responses = null;
        for (; ; ) {
            if (inboundFrame == null) {
                if (receiveBuff.readableBytes() < INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES) {
                    break;
                }

                int size = receiveBuff.readInt();
                int frameFlags = receiveBuff.readInt();

                if ((frameFlags & FLAG_OP_RESPONSE) == 0) {
                    inboundFrame = requestFrameAllocator.allocate(size);
                } else {
                    inboundFrame = remoteResponseFrameAllocator.allocate(size);
                }
                inboundFrame.byteBuffer().limit(size);
                inboundFrame.writeInt(size);
                inboundFrame.writeInt(frameFlags);
                inboundFrame.channel = this;
            }

            if (inboundFrame.remaining() > receiveBuff.readableBytes()) {
                ByteBuffer buffer = inboundFrame.byteBuffer();
                int oldLimit = buffer.limit();
                buffer.limit(buffer.position() + receiveBuff.readableBytes());
                receiveBuff.readBytes(buffer);
                buffer.limit(oldLimit);
            } else {
                receiveBuff.readBytes(inboundFrame.byteBuffer());
            }

            if (!inboundFrame.isComplete()) {
                break;
            }

            inboundFrame.complete();
            framesRead.inc();

            if (inboundFrame.isFlagRaised(FLAG_OP_RESPONSE)) {
                inboundFrame.next = responses;
                responses = inboundFrame;
            } else {
                opScheduler.schedule(inboundFrame);
                // frameHandler.handleRequest(inboundFrame);
            }
            inboundFrame = null;
        }

        if (responses != null) {
            requestService.handleResponse(responses);
        }
    }
}
