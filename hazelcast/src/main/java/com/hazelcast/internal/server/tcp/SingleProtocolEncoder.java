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

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.internal.nio.Protocols.UNEXPECTED_PROTOCOL;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

/**
 * Together with {@link SingleProtocolDecoder}, this encoder decoder pair is
 * used for checking correct protocol is used or not. {@link
 * SingleProtocolDecoder} checks if the correct protocol is received. If the
 * protocol is correct, both encoder and decoder swaps itself with the next
 * handler in the pipeline. If it isn't {@link SingleProtocolEncoder} throws
 * {@link ProtocolException} and {@link SingleProtocolDecoder} sends {@value
 * Protocols#UNEXPECTED_PROTOCOL}. Note that in client mode {@link
 * SingleProtocolEncoder} has no effect, and it swaps itself with the next
 * handler.
 */
public class SingleProtocolEncoder extends OutboundHandler<Void, ByteBuffer> {
    private final OutboundHandler[] outboundHandlers;

    private boolean isDecoderVerifiedProtocol;
    private boolean isDecoderReceivedProtocol;
    private boolean clusterProtocolBuffered;

    private String exceptionMessage;

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
            if (!isDecoderReceivedProtocol && !channel.isClientMode()) {
                return CLEAN;
            }

            // Decoder didn't verify the protocol, protocol error should be sent
            if (!isDecoderVerifiedProtocol && !channel.isClientMode()) {
                if (!sendProtocol()) {
                    return DIRTY;
                }
                // UNEXPECTED_PROTOCOL is sent (or at least in the socket
                // buffer). We can now throw exception in the pipeline to close
                // the channel.
                throw new ProtocolException(exceptionMessage);
            }

            if (channel.isClientMode()) {
                // Set up the next encoder in the pipeline if in client mode
                setupNextEncoder();
            }

            return CLEAN;
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
    protected void setupNextEncoder() {
        channel.outboundPipeline().replace(this, outboundHandlers);
    }

    @Override
    public void handlerAdded() {
        initDstBuffer(PROTOCOL_LENGTH);
    }

    private boolean isProtocolBufferDrained() {
        return dst.position() == 0;
    }

    // Used by SingleProtocolDecoder in order to swap
    // SingleProtocolEncoder with the next encoder in the pipeline
    public void signalProtocolVerified() {
        isDecoderReceivedProtocol = true;
        isDecoderVerifiedProtocol = true;
        channel.outboundPipeline().wakeup();
    }

    // Used by SingleProtocolDecoder in order to send HZX eventually
    public void signalWrongProtocol(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
        isDecoderReceivedProtocol = true;
        isDecoderVerifiedProtocol = false;
        channel.outboundPipeline().wakeup();
    }

    public OutboundHandler getFirstOutboundHandler() {
        return outboundHandlers[0];
    }
}
