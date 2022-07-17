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

import com.hazelcast.tpc.engine.iobuffer.IOBuffer;
import com.hazelcast.tpc.engine.iobuffer.IOBufferAllocator;
import io.netty.buffer.ByteBuf;
import com.hazelcast.tpc.engine.iouring.IOUringAsyncReadHandler;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.tpc.engine.iobuffer.IOBuffer.FLAG_OP_RESPONSE;

public class RequestIOUringReadHandler extends IOUringAsyncReadHandler {
    private IOBuffer inboundBuf;
    public IOBufferAllocator requestIOBufferAllocator;
    public IOBufferAllocator remoteResponseIOBufferAllocator;
    public Consumer<IOBuffer> responseHandler;
    public OpScheduler opScheduler;

    @Override
    public void onRead(ByteBuf buf) {
        IOBuffer responses = null;
        for (; ; ) {
            if (inboundBuf == null) {
                if (buf.readableBytes() < INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES) {
                    break;
                }

                int size = buf.readInt();
                int frameFlags = buf.readInt();

                if ((frameFlags & FLAG_OP_RESPONSE) == 0) {
                    inboundBuf = requestIOBufferAllocator.allocate(size);
                } else {
                    inboundBuf = remoteResponseIOBufferAllocator.allocate(size);
                }
                inboundBuf.byteBuffer().limit(size);
                inboundBuf.writeInt(size);
                inboundBuf.writeInt(frameFlags);
                inboundBuf.socket = socket;
            }

            if (inboundBuf.remaining() > buf.readableBytes()) {
                ByteBuffer buffer = inboundBuf.byteBuffer();
                int oldLimit = buffer.limit();
                buffer.limit(buffer.position() + buf.readableBytes());
                buf.readBytes(buffer);
                buffer.limit(oldLimit);
            } else {
                buf.readBytes(inboundBuf.byteBuffer());
            }

            if (!inboundBuf.isComplete()) {
                break;
            }

            inboundBuf.reconstructComplete();
            //framesRead.inc();

            if (inboundBuf.isFlagRaised(FLAG_OP_RESPONSE)) {
                inboundBuf.next = responses;
                responses = inboundBuf;
            } else {
                opScheduler.schedule(inboundBuf);
                // frameHandler.handleRequest(inboundFrame);
            }
            inboundBuf = null;
        }

        if (responses != null) {
            responseHandler.accept(responses);
        }
    }
}
