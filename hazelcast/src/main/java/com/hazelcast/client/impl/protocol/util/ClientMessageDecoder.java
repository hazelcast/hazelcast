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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.nio.InboundHandlerWithCounters;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.function.Consumer;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAGMENT_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAGMENT_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.FRAGMENTATION_ID_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;
import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.nio.IOUtil.compactOrClear;

/**
 * Builds {@link ClientMessage}s from byte chunks.
 *
 * Fragmented messages are merged into single messages before processed.
 */
public class ClientMessageDecoder extends InboundHandlerWithCounters<ByteBuffer, Consumer<ClientMessage>> {

    private final Connection connection;
    private final Long2ObjectHashMap<ClientMessageReader> builderBySessionIdMap = new Long2ObjectHashMap<>();
    private ClientMessageReader activeReader = new ClientMessageReader();

    public ClientMessageDecoder(Connection connection, Consumer<ClientMessage> dst) {
        dst(dst);
        this.connection = connection;
    }

    @Override
    public void handlerAdded() {
        initSrcBuffer();
    }

    @Override
    public HandlerStatus onRead() {
        src.flip();
        try {
            while (src.hasRemaining()) {
                boolean complete = activeReader.readFrom(src);
                if (!complete) {
                    break;
                }

                ClientMessage.Frame firstFrame = activeReader.getFrames().get(0);
                int flags = firstFrame.flags;
                if (ClientMessage.isFlagSet(flags, UNFRAGMENTED_MESSAGE)) {
                    handleMessage(activeReader);
                } else {
                    //remove the fragmentationFrame
                    activeReader.getFrames().removeFirst();
                    long fragmentationId = Bits.readLongL(firstFrame.content, FRAGMENTATION_ID_OFFSET);
                    if (ClientMessage.isFlagSet(flags, BEGIN_FRAGMENT_FLAG)) {
                        builderBySessionIdMap.put(fragmentationId, activeReader);
                    } else if (ClientMessage.isFlagSet(flags, END_FRAGMENT_FLAG)) {
                        ClientMessageReader messageReader = builderBySessionIdMap.get(fragmentationId);
                        LinkedList<ClientMessage.Frame> frames = messageReader.getFrames();
                        frames.addAll(activeReader.getFrames());
                        handleMessage(messageReader);
                    } else {
                        ClientMessageReader messageReader = builderBySessionIdMap.get(fragmentationId);
                        messageReader.getFrames().addAll(activeReader.getFrames());
                    }
                }

                activeReader = new ClientMessageReader();
            }

            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

    private void handleMessage(ClientMessageReader clientMessageReader) {
        LinkedList<ClientMessage.Frame> frames = clientMessageReader.getFrames();
        ClientMessage clientMessage = ClientMessage.createForDecode(frames);
        clientMessage.setConnection(connection);
        normalPacketsRead.inc();
        dst.accept(clientMessage);
    }

}
