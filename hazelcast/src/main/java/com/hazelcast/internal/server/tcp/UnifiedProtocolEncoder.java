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

import com.hazelcast.client.impl.protocol.util.ClientMessageEncoder;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.nio.ascii.TextEncoder;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Protocols.CLIENT_BINARY;
import static com.hazelcast.internal.nio.Protocols.CLUSTER;
import static com.hazelcast.internal.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.internal.nio.ascii.TextEncoder.TEXT_ENCODER;
import static com.hazelcast.internal.server.ServerContext.KILO_BYTE;
import static com.hazelcast.internal.util.JVMUtil.upcast;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_CLIENT_SEND_BUFFER_SIZE;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_SEND_BUFFER_SIZE;

/**
 * The ProtocolEncoder is responsible for writing the protocol and once the protocol
 * has been written, the ProtocolEncoder is replaced by the appropriate handler.
 *
 * The ProtocolEncoder and the 'client' side of a member connection, will always
 * write the cluster protocol immediately. The ProtocolEncoder on the 'server' side
 * of the connection will wait till it has received the protocol and then will only
 * send the protocol if the client side was a member.
 */
public class UnifiedProtocolEncoder
        extends OutboundHandler<Void, ByteBuffer> {

    private final ServerContext serverContext;
    private final HazelcastProperties props;
    private volatile String inboundProtocol;
    private boolean clusterProtocolBuffered;
    private volatile boolean encoderCanReplace;

    public UnifiedProtocolEncoder(ServerContext serverContext) {
        this.serverContext = serverContext;
        this.props = serverContext.properties();
    }

    @Override
    public void handlerAdded() {
        initDstBuffer(PROTOCOL_LENGTH);

        if (channel.isClientMode()) {
            // from the clientSide of a connection, we always send the cluster protocol to a fellow member.
            inboundProtocol = CLUSTER;
        }
    }

    /**
     * Signals the ProtocolEncoder that the protocol is known. This call will be
     * made by the ProtocolDecoder as soon as it knows the inbound protocol.
     *
     * @param inboundProtocol
     */
    void signalProtocolEstablished(String inboundProtocol) {
        assert !channel.isClientMode() : "Signal protocol should only be made on channel in serverMode";
        this.inboundProtocol = inboundProtocol;
        channel.outboundPipeline().wakeup();
    }

    @Override
    public HandlerStatus onWrite() {
        compactOrClear(dst);

        try {
            if (inboundProtocol == null) {
                // deal with spurious calls; the protocol to send isn't known yet.
                return CLEAN;
            }

            if (CLUSTER.equals(inboundProtocol)) {
                // in case of a member, the cluster protocol needs to be send first before initializing the channel.

                if (!clusterProtocolBuffered) {
                    clusterProtocolBuffered = true;
                    dst.put(stringToBytes(CLUSTER));
                    // Return false because ProtocolEncoder is not ready yet; but first we need to flush protocol
                    return DIRTY;
                }

                if (!isProtocolBufferDrained()) {
                    // Return false because ProtocolEncoder is not ready yet; but first we need to flush protocol
                    return DIRTY;
                }

                if (encoderCanReplace) {
                    initChannelForCluster();
                }
            } else if (CLIENT_BINARY.equals(inboundProtocol)) {
                // in case of a client, the member will not send the member protocol
                if (encoderCanReplace) {
                    initChannelForClient();
                }
            } else {
                // in case of a text-client, the member will not send the member protocol
                if (encoderCanReplace) {
                    initChannelForText();
                }
            }

            return CLEAN;
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

    private void initChannelForCluster() {
        channel.options()
                .setOption(SO_SNDBUF, props.getInteger(SOCKET_SEND_BUFFER_SIZE) * KILO_BYTE);

        ServerConnection connection = (TcpServerConnection) channel.attributeMap().get(ServerConnection.class);
        OutboundHandler[] handlers = serverContext.createOutboundHandlers(EndpointQualifier.MEMBER, connection);
        channel.outboundPipeline().replace(this, handlers);
    }

    private void initChannelForClient() {
        channel.options()
                .setOption(SO_SNDBUF, clientSndBuf());

        channel.outboundPipeline().replace(this, new ClientMessageEncoder());
    }

    private void initChannelForText() {
        channel.options()
                .setOption(SO_SNDBUF, clientSndBuf());

        TextEncoder encoder = (TextEncoder) channel.attributeMap().remove(TEXT_ENCODER);
        channel.outboundPipeline().replace(this, encoder);
    }

    private int clientSndBuf() {
        int sndBuf = props.getInteger(SOCKET_CLIENT_SEND_BUFFER_SIZE);
        if (sndBuf == -1) {
            sndBuf = props.getInteger(SOCKET_SEND_BUFFER_SIZE);
        }
        return sndBuf * KILO_BYTE;
    }

    public void signalEncoderCanReplace() {
        encoderCanReplace = true;
        channel.outboundPipeline().wakeup();
    }
}
