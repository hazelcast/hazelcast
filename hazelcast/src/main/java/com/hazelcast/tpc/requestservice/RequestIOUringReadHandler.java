/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.tpc.requestservice;

import com.hazelcast.tpc.engine.frame.Frame;
import com.hazelcast.tpc.engine.frame.FrameAllocator;
import io.netty.buffer.ByteBuf;
import com.hazelcast.tpc.engine.iouring.IOUringAsyncReadHandler;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.tpc.engine.frame.Frame.FLAG_OP_RESPONSE;

public class RequestIOUringReadHandler extends IOUringAsyncReadHandler {
    private Frame inboundFrame;
    public FrameAllocator requestFrameAllocator;
    public FrameAllocator remoteResponseFrameAllocator;
    public Consumer<Frame> responseHandler;
    public OpScheduler opScheduler;

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
                inboundFrame.socket = socket;
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

            inboundFrame.reconstructComplete();
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
            responseHandler.accept(responses);
        }
    }
}
