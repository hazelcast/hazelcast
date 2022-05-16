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
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.server.ServerConnection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Protocols.CLUSTER;
import static com.hazelcast.internal.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

/**
 * Writes the member protocol header bytes (HZC) to dst buffer and replaces itself by the next {@link OutboundHandler
 * OutboundHandlers}.
 */
public class MemberProtocolEncoder extends OutboundHandler<Void, ByteBuffer> {

    private final OutboundHandler[] outboundHandlers;

    /**
     * @param next the {@link OutboundHandler} to replace this one in the outbound pipeline
     *             upon match of protocol bytes
     */
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public MemberProtocolEncoder(OutboundHandler... next) {
        this.outboundHandlers = next;
    }

    @Override
    public void handlerAdded() {
        initDstBuffer(PROTOCOL_LENGTH, stringToBytes(CLUSTER));
    }

    @Override
    public HandlerStatus onWrite() {
        compactOrClear(dst);

        try {
            if (isProtocolBufferDrained()) {
                // replace!
                ServerConnection connection = (TcpServerConnection) channel.attributeMap().get(ServerConnection.class);
                connection.setConnectionType(ConnectionType.MEMBER);
                channel.outboundPipeline().replace(this, outboundHandlers);
                return CLEAN;
            }

            return DIRTY;
        } finally {
            upcast(dst).flip();
        }
    }

    /**
     * Checks if the protocol bytes have been drained.
     *
     * The protocol buffer is in write mode, so if position is 0, the protocol
     * buffer has been drained.
     *
     * @return true if the protocol buffer has been drained.
     */
    private boolean isProtocolBufferDrained() {
        return dst.position() == 0;
    }
}
