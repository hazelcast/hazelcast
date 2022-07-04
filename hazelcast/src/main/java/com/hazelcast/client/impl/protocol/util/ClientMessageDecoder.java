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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEndpointManager;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.nio.InboundHandlerWithCounters;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.util.collection.Long2ObjectHashMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.function.Consumer;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAGMENT_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAGMENT_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.FRAGMENTATION_ID_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.util.JVMUtil.upcast;

/**
 * Builds {@link ClientMessage}s from byte chunks.
 * <p>
 * Fragmented messages are merged into single messages before processed.
 */
public class ClientMessageDecoder extends InboundHandlerWithCounters<ByteBuffer, Consumer<ClientMessage>> {

    final Long2ObjectHashMap<ClientMessage> builderBySessionIdMap = new Long2ObjectHashMap<>();
    private final Connection connection;
    private ClientMessageReader activeReader;

    private boolean clientIsTrusted;
    private final int maxMessageLength;
    private final ClientEndpointManager clientEndpointManager;

    public ClientMessageDecoder(Connection connection, Consumer<ClientMessage> dst, HazelcastProperties properties) {
        dst(dst);
        if (properties == null) {
            properties = new HazelcastProperties((Properties) null);
        }
        clientEndpointManager = dst instanceof ClientEngine ? ((ClientEngine) dst).getEndpointManager() : null;
        maxMessageLength = properties.getInteger(ClusterProperty.CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES);
        activeReader = new ClientMessageReader(maxMessageLength);
        this.connection = connection;
    }

    @Override
    public void handlerAdded() {
        initSrcBuffer();
    }

    @Override
    public HandlerStatus onRead() {
        upcast(src).flip();
        try {
            while (src.hasRemaining()) {
                boolean trusted = isEndpointTrusted();
                boolean complete = activeReader.readFrom(src, trusted);
                if (!complete) {
                    break;
                }

                ClientMessage.Frame firstFrame = activeReader.getClientMessage().getStartFrame();
                int flags = firstFrame.flags;
                if (ClientMessage.isFlagSet(flags, UNFRAGMENTED_MESSAGE)) {
                    handleMessage(activeReader.getClientMessage());
                } else if (!trusted) {
                    throw new IllegalStateException(
                            "Fragmented client messages are not allowed before the client is authenticated.");
                } else {
                    ClientMessage message = activeReader.getClientMessage();
                    message.dropFragmentationFrame();
                    long fragmentationId = Bits.readLongL(firstFrame.content, FRAGMENTATION_ID_OFFSET);
                    if (ClientMessage.isFlagSet(flags, BEGIN_FRAGMENT_FLAG)) {
                        builderBySessionIdMap.put(fragmentationId, message);
                    } else if (ClientMessage.isFlagSet(flags, END_FRAGMENT_FLAG)) {
                        ClientMessage clientMessage = mergeIntoExistingClientMessage(fragmentationId);
                        builderBySessionIdMap.remove(fragmentationId);
                        handleMessage(clientMessage);
                    } else {
                        mergeIntoExistingClientMessage(fragmentationId);
                    }
                }

                activeReader = new ClientMessageReader(maxMessageLength);
            }

            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

    private boolean isEndpointTrusted() {
        if (clientEndpointManager == null || clientIsTrusted) {
            return true;
        }
        ClientEndpoint endpoint = clientEndpointManager.getEndpoint(connection);
        clientIsTrusted = endpoint != null && endpoint.isAuthenticated();
        return clientIsTrusted;
    }

    private ClientMessage mergeIntoExistingClientMessage(long fragmentationId) {
        ClientMessage existingMessage = builderBySessionIdMap.get(fragmentationId);
        existingMessage.merge(activeReader.getClientMessage());
        return existingMessage;
    }

    private void handleMessage(ClientMessage clientMessage) {
        clientMessage.setConnection(connection);
        normalPacketsRead.inc();
        dst.accept(clientMessage);
    }

}
