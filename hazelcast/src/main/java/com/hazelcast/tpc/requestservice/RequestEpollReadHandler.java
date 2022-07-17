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
import com.hazelcast.tpc.engine.epoll.EpollAsyncSocket;
import com.hazelcast.tpc.engine.epoll.EpollReadHandler;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.tpc.engine.iobuffer.IOBuffer.FLAG_OP_RESPONSE;

public class RequestEpollReadHandler extends EpollReadHandler {

    public OpScheduler opScheduler;
    public Consumer<IOBuffer> responseHandler;
    public IOBufferAllocator requestIOBufferAllocator;
    public IOBufferAllocator remoteResponseIOBufferAllocator;
    private IOBuffer inboundBuf;
    private EpollAsyncSocket asyncSocket;

    @Override
    public void init(EpollAsyncSocket asyncSocket) {
        this.asyncSocket = asyncSocket;
    }

    @Override
    public void onRead(ByteBuffer receiveBuffer) {
        IOBuffer responseChain = null;
        for (; ; ) {
            if (inboundBuf == null) {
                if (receiveBuffer.remaining() < INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES) {
                    break;
                }

                int size = receiveBuffer.getInt();
                int flags = receiveBuffer.getInt();
                if ((flags & FLAG_OP_RESPONSE) == 0) {
                    inboundBuf = requestIOBufferAllocator.allocate(size);
                } else {
                    inboundBuf = remoteResponseIOBufferAllocator.allocate(size);
                }
                inboundBuf.byteBuffer().limit(size);
                inboundBuf.writeInt(size);
                inboundBuf.writeInt(flags);
                inboundBuf.socket = asyncSocket;
            }

            int size = inboundBuf.size();
            int remaining = size - inboundBuf.position();
            inboundBuf.write(receiveBuffer, remaining);

            if (!inboundBuf.isComplete()) {
                break;
            }

            inboundBuf.reconstructComplete();
            inboundBuf = null;
            //framesRead.inc();

            if (inboundBuf.isFlagRaised(FLAG_OP_RESPONSE)) {
                inboundBuf.next = responseChain;
                responseChain = inboundBuf;
            } else {
                opScheduler.schedule(inboundBuf);
            }
        }

        if (responseChain != null) {
            responseHandler.accept(responseChain);
        }
    }
}
