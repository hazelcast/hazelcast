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

package com.hazelcast.nio.tcp;

import com.hazelcast.client.impl.protocol.util.ClientMessageDecoder;
import com.hazelcast.config.MemcacheProtocolConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.ChannelOptions;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.ascii.MemcacheTextDecoder;
import com.hazelcast.nio.ascii.RestApiTextDecoder;
import com.hazelcast.nio.ascii.TextDecoder;
import com.hazelcast.nio.ascii.TextEncoder;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.compactOrClear;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.nio.Protocols.PROTOCOL_LENGTH;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_CLIENT_RECEIVE_BUFFER_SIZE;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_RECEIVE_BUFFER_SIZE;
import static com.hazelcast.internal.util.StringUtil.bytesToString;
import static com.hazelcast.internal.util.StringUtil.stringToBytes;

/**
 * A {@link InboundHandler} that reads the protocol bytes
 * {@link com.hazelcast.nio.Protocols} and based on the protocol it creates the
 * appropriate handlers.
 *
 * The ProtocolDecoder doesn't forward to the dst; it replaces itself once the
 * protocol bytes are known. So that is why the Void type for dst.
 */
public class UnifiedProtocolDecoder
        extends InboundHandler<ByteBuffer, Void> {

    private final IOService ioService;
    private final UnifiedProtocolEncoder protocolEncoder;
    private final HazelcastProperties props;

    public UnifiedProtocolDecoder(IOService ioService, UnifiedProtocolEncoder protocolEncoder) {
        this.ioService = ioService;
        this.protocolEncoder = protocolEncoder;
        this.props = ioService.properties();
    }

    @Override
    public void handlerAdded() {
        initSrcBuffer(PROTOCOL_LENGTH);
    }

    @Override
    public HandlerStatus onRead() throws Exception {
        src.flip();

        try {
            if (src.remaining() < PROTOCOL_LENGTH) {
                // The protocol has not yet been fully received.
                return CLEAN;
            }

            String protocol = loadProtocol();

            if (CLUSTER.equals(protocol)) {
                initChannelForCluster();
            } else if (CLIENT_BINARY_NEW.equals(protocol)) {
                initChannelForClient();
            } else if (RestApiTextDecoder.TEXT_PARSERS.isCommandPrefix(protocol)) {
                RestApiConfig restApiConfig = ioService.getRestApiConfig();
                if (!restApiConfig.isEnabledAndNotEmpty()) {
                    throw new IllegalStateException("REST API is not enabled.");
                }
                initChannelForText(protocol, true);
            } else if (MemcacheTextDecoder.TEXT_PARSERS.isCommandPrefix(protocol)) {
                MemcacheProtocolConfig memcacheProtocolConfig = ioService.getMemcacheProtocolConfig();
                if (! memcacheProtocolConfig.isEnabled()) {
                    throw new IllegalStateException("Memcache text protocol is not enabled.");
                }
                // text doesn't have a protocol; anything that isn't cluster/client protocol will be interpreted as txt.
                initChannelForText(protocol, false);
            } else {
                throw new IllegalStateException("Unknown protocol: " + protocol);
            }

            if (!channel.isClientMode()) {
                protocolEncoder.signalProtocolEstablished(protocol);
            }

            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

    private String loadProtocol() {
        byte[] protocolBytes = new byte[PROTOCOL_LENGTH];
        src.get(protocolBytes);
        if (isTlsHandshake(protocolBytes)) {
            // fail-fast
            throw new IllegalStateException("TLS handshake header detected, but plain protocol header was expected.");
        }
        return bytesToString(protocolBytes);
    }

    private void initChannelForCluster() {
        channel.options()
                .setOption(SO_SNDBUF, props.getInteger(SOCKET_RECEIVE_BUFFER_SIZE) * KILO_BYTE);

        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        connection.setType(ConnectionType.MEMBER);
        channel.inboundPipeline().replace(this, ioService.createInboundHandlers(EndpointQualifier.MEMBER, connection));
    }

    private void initChannelForClient() {
        channel.options()
                .setOption(SO_RCVBUF, clientRcvBuf())
                // clients dont support direct buffers
                .setOption(DIRECT_BUF, false);

        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        channel.inboundPipeline().replace(this, new ClientMessageDecoder(connection, ioService.getClientEngine()));
    }

    private void initChannelForText(String protocol, boolean restApi) {
        ChannelOptions config = channel.options();

        config.setOption(SO_RCVBUF, clientRcvBuf());

        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);

        TextEncoder encoder = new TextEncoder(connection);

        channel.attributeMap().put(TextEncoder.TEXT_ENCODER, encoder);

        TextDecoder decoder = restApi
                ? new RestApiTextDecoder(connection, encoder, false)
                : new MemcacheTextDecoder(connection, encoder, false);
        decoder.src(newByteBuffer(config.getOption(SO_RCVBUF), config.getOption(DIRECT_BUF)));
        // we need to restore whatever is read
        decoder.src().put(stringToBytes(protocol));

        channel.inboundPipeline().replace(this, decoder);
    }

    private int clientRcvBuf() {
        int rcvBuf = props.getInteger(SOCKET_CLIENT_RECEIVE_BUFFER_SIZE);
        if (rcvBuf == -1) {
            rcvBuf = props.getInteger(SOCKET_RECEIVE_BUFFER_SIZE);
        }
        return rcvBuf * KILO_BYTE;
    }

    // SSLv3 https://tools.ietf.org/html/rfc6101#section-5.2
    // TLSv1.0 https://tools.ietf.org/html/rfc2246
    // TLSv1.1 https://tools.ietf.org/html/rfc4346#section-6.2
    // TLSv1.2 https://tools.ietf.org/html/rfc5246#section-6.2
    // TLSv1.3 https://tools.ietf.org/html/rfc8446#section-5.1
    // Tested TLS Protocol bytes: 22 (handshake type), 3 (protocolVersion.major), 0-3 (ProtocolVersion.minor)
    @SuppressWarnings("checkstyle:magicnumber")
    private static boolean isTlsHandshake(byte[] protocolBytes) {
        return protocolBytes[0] == 22 && protocolBytes[1] == 3 && protocolBytes[2] >= 0 && protocolBytes[2] <= 3;
    }

}
