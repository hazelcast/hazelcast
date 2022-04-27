package com.hazelcast.spi.impl.requestservice;

import com.hazelcast.spi.impl.engine.frame.Frame;
import com.hazelcast.spi.impl.engine.frame.FrameAllocator;
import com.hazelcast.spi.impl.engine.nio.NioAsyncSocket;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.spi.impl.engine.frame.Frame.FLAG_OP_RESPONSE;

public class RequestNioChannel extends NioAsyncSocket {

    private Frame inboundFrame;
    public FrameAllocator requestFrameAllocator;
    public FrameAllocator remoteResponseFrameAllocator;
    public OpScheduler opScheduler;
    public RequestService requestService;

    @Override
    public void handleRead(ByteBuffer receiveBuffer) {
        Frame responseChain = null;
        for (; ; ) {
            if (inboundFrame == null) {
                if (receiveBuffer.remaining() < INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES) {
                    break;
                }

                int size = receiveBuffer.getInt();
                int flags = receiveBuffer.getInt();
                if ((flags & FLAG_OP_RESPONSE) == 0) {
                    inboundFrame = requestFrameAllocator.allocate(size);
                } else {
                    inboundFrame = remoteResponseFrameAllocator.allocate(size);
                }
                inboundFrame.byteBuffer().limit(size);
                inboundFrame.writeInt(size);
                inboundFrame.writeInt(flags);
                inboundFrame.channel = this;
            }

            int size = inboundFrame.size();
            int remaining = size - inboundFrame.position();
            inboundFrame.write(receiveBuffer, remaining);

            if (!inboundFrame.isComplete()) {
                break;
            }

            inboundFrame.complete();
            framesRead.inc();

            if (inboundFrame.isFlagRaised(FLAG_OP_RESPONSE)) {
                inboundFrame.next = responseChain;
                responseChain = inboundFrame;
            } else {
                opScheduler.schedule(inboundFrame);
            }
            inboundFrame = null;
        }

        if (responseChain != null) {
            requestService.handleResponse(responseChain);
        }
    }
}
