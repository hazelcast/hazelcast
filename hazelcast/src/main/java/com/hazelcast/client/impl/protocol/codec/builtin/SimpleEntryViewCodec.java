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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;

public final class SimpleEntryViewCodec {
    private static final int COST_OFFSET = 0;
    private static final int CREATION_TIME_OFFSET = COST_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int EXPIRATION_TIME_OFFSET = CREATION_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int HITS_OFFSET = EXPIRATION_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int LAST_ACCESS_TIME_OFFSET = HITS_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int LAST_STORED_TIME_OFFSET = LAST_ACCESS_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int LAST_UPDATE_TIME_OFFSET = LAST_STORED_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int VERSION_OFFSET = LAST_UPDATE_TIME_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int TTL_OFFSET = VERSION_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int MAX_IDLE_OFFSET = TTL_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = MAX_IDLE_OFFSET + Bits.LONG_SIZE_IN_BYTES;

    private SimpleEntryViewCodec() {
    }

    public static void encode(ClientMessage clientMessage, SimpleEntryView<Data, Data> entryView) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, COST_OFFSET, entryView.getCost());
        encodeLong(initialFrame.content, CREATION_TIME_OFFSET, entryView.getCreationTime());
        encodeLong(initialFrame.content, EXPIRATION_TIME_OFFSET, entryView.getExpirationTime());
        encodeLong(initialFrame.content, HITS_OFFSET, entryView.getHits());
        encodeLong(initialFrame.content, LAST_ACCESS_TIME_OFFSET, entryView.getLastAccessTime());
        encodeLong(initialFrame.content, LAST_STORED_TIME_OFFSET, entryView.getLastStoredTime());
        encodeLong(initialFrame.content, LAST_UPDATE_TIME_OFFSET, entryView.getLastUpdateTime());
        encodeLong(initialFrame.content, VERSION_OFFSET, entryView.getVersion());
        encodeLong(initialFrame.content, TTL_OFFSET, entryView.getTtl());
        encodeLong(initialFrame.content, MAX_IDLE_OFFSET, entryView.getMaxIdle());
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, entryView.getKey());
        DataCodec.encode(clientMessage, entryView.getValue());

        clientMessage.add(END_FRAME);
    }

    public static SimpleEntryView<Data, Data> decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();
        ClientMessage.Frame initialFrame = iterator.next();

        long cost = decodeLong(initialFrame.content, COST_OFFSET);
        long creationTime = decodeLong(initialFrame.content, CREATION_TIME_OFFSET);
        long expirationTime = decodeLong(initialFrame.content, EXPIRATION_TIME_OFFSET);
        long hits = decodeLong(initialFrame.content, HITS_OFFSET);
        long lastAccessTime = decodeLong(initialFrame.content, LAST_ACCESS_TIME_OFFSET);
        long lastStoredTime = decodeLong(initialFrame.content, LAST_STORED_TIME_OFFSET);
        long lastUpdateTime = decodeLong(initialFrame.content, LAST_UPDATE_TIME_OFFSET);
        long version = decodeLong(initialFrame.content, VERSION_OFFSET);
        long ttl = decodeLong(initialFrame.content, TTL_OFFSET);
        long maxIdle = decodeLong(initialFrame.content, MAX_IDLE_OFFSET);

        Data key = DataCodec.decode(iterator);
        Data value = DataCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        SimpleEntryView<Data, Data> entryView = new SimpleEntryView<>(key, value);
        entryView.setCost(cost);
        entryView.setCreationTime(creationTime);
        entryView.setExpirationTime(expirationTime);
        entryView.setHits(hits);
        entryView.setLastAccessTime(lastAccessTime);
        entryView.setLastStoredTime(lastStoredTime);
        entryView.setLastUpdateTime(lastUpdateTime);
        entryView.setVersion(version);
        entryView.setTtl(ttl);
        entryView.setMaxIdle(maxIdle);
        return entryView;
    }
}
