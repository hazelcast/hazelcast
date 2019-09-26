/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.nio.ConnectionType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.internal.util.StringUtil.bytesToString;

public class SingleProtocolDecoder
        extends InboundHandler<ByteBuffer, Void> {

    protected final InboundHandler[] inboundHandlers;
    protected final ProtocolType supportedProtocol;

    private final MemberProtocolEncoder encoder;

    public SingleProtocolDecoder(ProtocolType supportedProtocol, InboundHandler next) {
        this(supportedProtocol, new InboundHandler[] {next}, null);
    }

    /**
     * Decodes first 3 incoming bytes, validates against {@code supportedProtocol} and, when
     * matching, replaces itself in the inbound pipeline with the {@code next InboundHandler}s.
     *
     * @param supportedProtocol the {@link ProtocolType} supported by this {@code ProtocolDecoder}
     * @param next              the {@link InboundHandler}s to replace this one in the inbound pipeline
     *                          upon match of protocol bytes
     * @param encoder           a {@link OutboundHandler} that will be notified when matching protocol
     *                          bytes have been received
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public SingleProtocolDecoder(ProtocolType supportedProtocol, InboundHandler[] next, MemberProtocolEncoder encoder) {
        this.supportedProtocol = supportedProtocol;
        this.inboundHandlers = next;
        this.encoder = encoder;
    }

    @Override
    public void handlerAdded() {
        initSrcBuffer(PROTOCOL_LENGTH);
    }

    @Override
    public HandlerStatus onRead() {
        src.flip();

        try {
            if (src.remaining() < PROTOCOL_LENGTH) {
                // The protocol has not yet been fully received.
                return CLEAN;
            }

            verifyProtocol(loadProtocol());
            // initialize the connection
            initConnection();
            setupNextDecoder();

            if (shouldSignalProtocolLoaded()) {
                encoder.signalProtocolLoaded();
            }

            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

    protected void setupNextDecoder() {
        // replace this handler with the next one
        channel.inboundPipeline().replace(this, inboundHandlers);
    }

    protected void verifyProtocol(String incomingProtocol) {
        if (!incomingProtocol.equals(supportedProtocol.getDescriptor())) {
            throw new IllegalStateException("Unsupported protocol exchange detected, "
                    + "expected protocol: " + supportedProtocol.name());
        }
    }

    private String loadProtocol() {
        byte[] protocolBytes = new byte[PROTOCOL_LENGTH];
        src.get(protocolBytes);
        return bytesToString(protocolBytes);
    }

    private void initConnection() {
        if (supportedProtocol == ProtocolType.MEMBER) {
            TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
            connection.setType(ConnectionType.MEMBER);
        }
    }

    private boolean shouldSignalProtocolLoaded() {
        return !channel.isClientMode() && (encoder != null);
    }
}
