package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;
import io.netty.buffer.ByteBuf;
import com.hazelcast.tpc.engine.iouring.IOUringAsyncSocket;
import com.hazelcast.tpc.engine.iouring.IOUringReadHandler;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.tpc.engine.frame.Frame.FLAG_OP_RESPONSE;

public class RequestIOUringReadHandler implements IOUringReadHandler {
    private Frame inboundFrame;
    public FrameAllocator requestFrameAllocator;
    public FrameAllocator remoteResponseFrameAllocator;
    public RequestService requestService;
    public OpScheduler opScheduler;
    private IOUringAsyncSocket asyncSocket;

    @Override
    public void init(IOUringAsyncSocket asyncSocket) {
        this.asyncSocket = asyncSocket;
    }

    @Override
    public void onRead(ByteBuf buf) {
        Frame responses = null;
        for (; ; ) {
            if (inboundFrame == null) {
                if (buf.readableBytes() < INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES) {
                    break;
                }

                int size = buf.readInt();
                int frameFlags = buf.readInt();

                if ((frameFlags & FLAG_OP_RESPONSE) == 0) {
                    inboundFrame = requestFrameAllocator.allocate(size);
                } else {
                    inboundFrame = remoteResponseFrameAllocator.allocate(size);
                }
                inboundFrame.byteBuffer().limit(size);
                inboundFrame.writeInt(size);
                inboundFrame.writeInt(frameFlags);
                inboundFrame.socket = asyncSocket;
            }

            if (inboundFrame.remaining() > buf.readableBytes()) {
                ByteBuffer buffer = inboundFrame.byteBuffer();
                int oldLimit = buffer.limit();
                buffer.limit(buffer.position() + buf.readableBytes());
                buf.readBytes(buffer);
                buffer.limit(oldLimit);
            } else {
                buf.readBytes(inboundFrame.byteBuffer());
            }

            if (!inboundFrame.isComplete()) {
                break;
            }

            inboundFrame.complete();
            //framesRead.inc();

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
