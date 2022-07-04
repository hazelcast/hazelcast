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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("56693dd0a97e71b470189a8c7a9df97a")
public final class SimpleEntryViewCodec {
    private static final int COST_FIELD_OFFSET = 0;
    private static final int CREATION_TIME_FIELD_OFFSET = COST_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int EXPIRATION_TIME_FIELD_OFFSET = CREATION_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int HITS_FIELD_OFFSET = EXPIRATION_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int LAST_ACCESS_TIME_FIELD_OFFSET = HITS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int LAST_STORED_TIME_FIELD_OFFSET = LAST_ACCESS_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int LAST_UPDATE_TIME_FIELD_OFFSET = LAST_STORED_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int VERSION_FIELD_OFFSET = LAST_UPDATE_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int TTL_FIELD_OFFSET = VERSION_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int MAX_IDLE_FIELD_OFFSET = TTL_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = MAX_IDLE_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private SimpleEntryViewCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.map.impl.SimpleEntryView<com.hazelcast.internal.serialization.Data, com.hazelcast.internal.serialization.Data> simpleEntryView) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, COST_FIELD_OFFSET, simpleEntryView.getCost());
        encodeLong(initialFrame.content, CREATION_TIME_FIELD_OFFSET, simpleEntryView.getCreationTime());
        encodeLong(initialFrame.content, EXPIRATION_TIME_FIELD_OFFSET, simpleEntryView.getExpirationTime());
        encodeLong(initialFrame.content, HITS_FIELD_OFFSET, simpleEntryView.getHits());
        encodeLong(initialFrame.content, LAST_ACCESS_TIME_FIELD_OFFSET, simpleEntryView.getLastAccessTime());
        encodeLong(initialFrame.content, LAST_STORED_TIME_FIELD_OFFSET, simpleEntryView.getLastStoredTime());
        encodeLong(initialFrame.content, LAST_UPDATE_TIME_FIELD_OFFSET, simpleEntryView.getLastUpdateTime());
        encodeLong(initialFrame.content, VERSION_FIELD_OFFSET, simpleEntryView.getVersion());
        encodeLong(initialFrame.content, TTL_FIELD_OFFSET, simpleEntryView.getTtl());
        encodeLong(initialFrame.content, MAX_IDLE_FIELD_OFFSET, simpleEntryView.getMaxIdle());
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, simpleEntryView.getKey());
        DataCodec.encode(clientMessage, simpleEntryView.getValue());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.map.impl.SimpleEntryView<com.hazelcast.internal.serialization.Data, com.hazelcast.internal.serialization.Data> decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long cost = decodeLong(initialFrame.content, COST_FIELD_OFFSET);
        long creationTime = decodeLong(initialFrame.content, CREATION_TIME_FIELD_OFFSET);
        long expirationTime = decodeLong(initialFrame.content, EXPIRATION_TIME_FIELD_OFFSET);
        long hits = decodeLong(initialFrame.content, HITS_FIELD_OFFSET);
        long lastAccessTime = decodeLong(initialFrame.content, LAST_ACCESS_TIME_FIELD_OFFSET);
        long lastStoredTime = decodeLong(initialFrame.content, LAST_STORED_TIME_FIELD_OFFSET);
        long lastUpdateTime = decodeLong(initialFrame.content, LAST_UPDATE_TIME_FIELD_OFFSET);
        long version = decodeLong(initialFrame.content, VERSION_FIELD_OFFSET);
        long ttl = decodeLong(initialFrame.content, TTL_FIELD_OFFSET);
        long maxIdle = decodeLong(initialFrame.content, MAX_IDLE_FIELD_OFFSET);

        com.hazelcast.internal.serialization.Data key = DataCodec.decode(iterator);
        com.hazelcast.internal.serialization.Data value = DataCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createSimpleEntryView(key, value, cost, creationTime, expirationTime, hits, lastAccessTime, lastStoredTime, lastUpdateTime, version, ttl, maxIdle);
    }
}
