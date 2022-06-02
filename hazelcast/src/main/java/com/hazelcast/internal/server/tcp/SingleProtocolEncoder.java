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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.nio.Protocols;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.BLOCKED;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.internal.nio.Protocols.UNEXPECTED_PROTOCOL;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

/**
 * Together with {@link SingleProtocolDecoder}, this encoder-decoder pair is used to check if correct protocol is used.
 * {@link SingleProtocolDecoder} checks if the proper protocol is received. If the protocol is correct, both encoder and decoder
 * are replaced by the next handlers in the pipeline. If it isn't the {@link SingleProtocolEncoder} sends
 * {@link Protocols#UNEXPECTED_PROTOCOL} response and throws a {@link ProtocolException}. Note that in client mode the
 * {@link SingleProtocolEncoder} allows blocking packet writes until the (member-)protocol is confirmed.
 */
public class SingleProtocolEncoder extends OutboundHandler<Void, ByteBuffer> {
    private final OutboundHandler[] outboundHandlers;

    private boolean clusterProtocolBuffered;

    private volatile boolean isDecoderVerifiedProtocol;
    private volatile boolean isDecoderReceivedProtocol;
    private volatile String exceptionMessage;

    public SingleProtocolEncoder(OutboundHandler next) {
        this(new OutboundHandler[]{next});
    }

    public SingleProtocolEncoder(OutboundHandler[] next) {
        this.outboundHandlers = next;
    }

    @Override
    public HandlerStatus onWrite() throws Exception {
        compactOrClear(dst);

        // Note that in client mode SingleProtocolEncoder never
        // sends anything and only swaps itself with the next encoder
        try {
            // First, decoder must receive the protocol
            if (!isDecoderReceivedProtocol) {
                return BLOCKED;
            }
            if (isDecoderVerifiedProtocol) {
                // Set up the next encoder in the pipeline once the protocol is verified
                setupNextEncoder();
                return CLEAN;
            }

            // Decoder received protocol bytes, but verification failed. If we are server/acceptor, then respond with the
            // UNEXPECTED_PROTOCOL response bytes.
            if (!channel.isClientMode()) {
                if (!sendProtocol()) {
                    return DIRTY;
                }
            }
            // Either we are in the client mode or the UNEXPECTED_PROTOCOL is sent already (or at least placed into the
            // destination buffer). We can now throw exception in the pipeline to close the channel.
            throw new ProtocolException(exceptionMessage);
        } finally {
            upcast(dst).flip();
        }
    }

    // Method used for sending HZX
    private boolean sendProtocol() {
        if (!clusterProtocolBuffered) {
            clusterProtocolBuffered = true;
            dst.put(stringToBytes(UNEXPECTED_PROTOCOL));
            return false;
        }

        return isProtocolBufferDrained();
    }

    // Swap this encoder with the next one
    private void setupNextEncoder() {
        channel.outboundPipeline().replace(this, outboundHandlers);
    }

    @Override
    public void handlerAdded() {
        initDstBuffer(PROTOCOL_LENGTH);
    }

    private boolean isProtocolBufferDrained() {
        return dst.position() == 0;
    }


    // The signal methods below are called from the protocol decoder
    // side that is run on IO input threads. We must synchronize the
    // accesses of the variables which these methods share with
    // SingleProtocolEncoder#onWrite that is run on IO output threads.

    // Used by SingleProtocolDecoder in order to swap
    // SingleProtocolEncoder with the next encoder in the pipeline
    public void signalProtocolVerified() {
        // this update order below must stay in reverse order with access order in SingleProtocolEncode#onWrite
        isDecoderVerifiedProtocol = true;
        isDecoderReceivedProtocol = true;
        // This channel can become null when SingleProtocolEncoder is not active handler of the outbound
        // pipeline, when the previous MemberProtocolEncoder doesn't replace itself with SingleProtocolEncoder
        // yet. In this case, this outboundPipeline().wakeup() call can be ignored since it is not possible
        // to enter the blocked state from the path that isDecoderReceivedProtocol check is performed.
        if (channel != null) {
            channel.outboundPipeline().wakeup();
        }
    }

    // Used by SingleProtocolDecoder in order to send HZX eventually
    public void signalWrongProtocol(String exceptionMessage) {
        // this update order below must stay in reverse order with access order in SingleProtocolEncode#onWrite
        this.exceptionMessage = exceptionMessage;
        isDecoderVerifiedProtocol = false;
        isDecoderReceivedProtocol = true;
        // This channel can become null when SingleProtocolEncoder is not active handler of the outbound
        // pipeline, when the previous MemberProtocolEncoder doesn't replace itself with SingleProtocolEncoder
        // yet. In this case, this outboundPipeline().wakeup() call can be ignored since it is not possible
        // to enter the blocked state from the path that isDecoderReceivedProtocol check is performed.
        if (channel != null) {
            channel.outboundPipeline().wakeup();
        }
    }

    public OutboundHandler getFirstOutboundHandler() {
        return outboundHandlers[0];
    }
}
