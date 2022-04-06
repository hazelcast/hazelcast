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

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.server.ServerConnection;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.internal.nio.Protocols.UNEXPECTED_PROTOCOL;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.internal.util.StringUtil.bytesToString;

/**
 * Checks if the correct protocol is received then swaps itself with the next
 * handler in the pipeline.
 * <p>
 * See also {@link SingleProtocolEncoder}
 * </p>
 */
public class SingleProtocolDecoder
        extends InboundHandler<ByteBuffer, Void> {

    protected final InboundHandler[] inboundHandlers;
    protected final ProtocolType supportedProtocol;
    /**
     * This flag is used to ensure that {@link #verifyProtocol(String)} is called only once
     * with initial bytes of connection. Formerly, this method would be called multiple times
     * with new incoming data, although it failed after its first call.
     */
    protected volatile boolean verifyProtocolCalled;
    final SingleProtocolEncoder encoder;

    public SingleProtocolDecoder(ProtocolType supportedProtocol, InboundHandler next, SingleProtocolEncoder encoder) {
        this(supportedProtocol, new InboundHandler[]{next}, encoder);
    }

    /**
     * Decodes first 3 incoming bytes, validates against {@code
     * supportedProtocol} and, when matching, replaces itself in the inbound
     * pipeline with the {@code next InboundHandler}s.
     *
     * @param supportedProtocol                 the {@link ProtocolType}
     *                                          supported by this {@code
     *                                          ProtocolDecoder}
     * @param next                              the {@link InboundHandler}s to
     *                                          replace this one in the inbound
     *                                          pipeline upon match of protocol
     *                                          bytes
     * @param encoder                           a {@link SingleProtocolEncoder}
     *                                          that will be notified when
     *                                          non-matching protocol bytes have
     *                                          been received
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public SingleProtocolDecoder(ProtocolType supportedProtocol, InboundHandler[] next,
                                 SingleProtocolEncoder encoder) {
        this.supportedProtocol = supportedProtocol;
        this.inboundHandlers = next;
        this.encoder = encoder;
        this.verifyProtocolCalled = false;
    }

    @Override
    public void handlerAdded() {
        initSrcBuffer(PROTOCOL_LENGTH);
    }

    @Override
    public HandlerStatus onRead() {
        upcast(src).flip();

        try {
            if (src.remaining() < PROTOCOL_LENGTH) {
                // The protocol has not yet been fully received.
                return CLEAN;
            }
            boolean verifyProtocolPreviouslyCalled = verifyProtocolCalled;
            if (verifyProtocolPreviouslyCalled || !verifyProtocol(loadProtocol())) {
                // The exception that will close the Connection eventually thrown
                // in SingleProtocolEncoder, since we send wrong protocol signal
                // for this in verifyProtocol.
                if (verifyProtocolPreviouslyCalled) {
                    // We do this trick to clear buffer on compactOrClear(ByteBuffer)
                    // which is called in the finally block below. Otherwise, the
                    // previous handler may get stuck in a DIRTY loop even if the
                    // channel closes. We observed this behavior in TLSDecoder
                    // before.
                    upcast(src).position(src.limit());
                }
                return CLEAN;
            }
            encoder.signalProtocolVerified();

            // Initialize the connection
            initConnection();
            setupNextDecoder();

            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

    protected void setupNextDecoder() {
        // Replace this handler with the next one
        channel.inboundPipeline().replace(this, inboundHandlers);
    }

    // Verify that received protocol is expected one.
    // If not then signal SingleProtocolEncoder and throw exception.
    protected boolean verifyProtocol(String incomingProtocol) {
        verifyProtocolCalled = true;
        if (!incomingProtocol.equals(supportedProtocol.getDescriptor())) {
            handleUnexpectedProtocol(incomingProtocol);
            encoder.signalWrongProtocol("Unsupported protocol exchange detected, expected protocol: "
                    + supportedProtocol.name() + ", actual protocol or first three bytes are: " + incomingProtocol);
            return false;
        }
        return true;
    }

    protected void handleUnexpectedProtocol(String incomingProtocol) {
        if (incomingProtocol.equals(UNEXPECTED_PROTOCOL)) {
            // We can throw exception here, and we don't need to signal the
            // encoder because when HZX is received there is no data to be
            // sent.
            throw new ProtocolException("Instance to be connected replied with"
                    + " HZX. This means a different protocol than expected sent"
                    + " to target instance");
        }
    }

    private String loadProtocol() {
        byte[] protocolBytes = new byte[PROTOCOL_LENGTH];
        src.get(protocolBytes);
        return bytesToString(protocolBytes);
    }

    private void initConnection() {
        if (supportedProtocol == ProtocolType.MEMBER) {
            TcpServerConnection connection = (TcpServerConnection) channel.attributeMap().get(ServerConnection.class);
            connection.setConnectionType(ConnectionType.MEMBER);
        }
    }
}
