/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

@SuppressWarnings("unused")
@Generated("86e2b9c454d334fc2aab7183f9a772d6")
public final class ReplicatedMapEntryViewHolderCodec {
    private static final int CREATION_TIME_FIELD_OFFSET = 0;
    private static final int HITS_FIELD_OFFSET = CREATION_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int LAST_ACCESS_TIME_FIELD_OFFSET = HITS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int LAST_UPDATE_TIME_FIELD_OFFSET = LAST_ACCESS_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int TTL_MILLIS_FIELD_OFFSET = LAST_UPDATE_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = TTL_MILLIS_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private ReplicatedMapEntryViewHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder replicatedMapEntryViewHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, CREATION_TIME_FIELD_OFFSET, replicatedMapEntryViewHolder.getCreationTime());
        encodeLong(initialFrame.content, HITS_FIELD_OFFSET, replicatedMapEntryViewHolder.getHits());
        encodeLong(initialFrame.content, LAST_ACCESS_TIME_FIELD_OFFSET, replicatedMapEntryViewHolder.getLastAccessTime());
        encodeLong(initialFrame.content, LAST_UPDATE_TIME_FIELD_OFFSET, replicatedMapEntryViewHolder.getLastUpdateTime());
        encodeLong(initialFrame.content, TTL_MILLIS_FIELD_OFFSET, replicatedMapEntryViewHolder.getTtlMillis());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, replicatedMapEntryViewHolder.getKey(), DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, replicatedMapEntryViewHolder.getValue(), DataCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long creationTime = decodeLong(initialFrame.content, CREATION_TIME_FIELD_OFFSET);
        long hits = decodeLong(initialFrame.content, HITS_FIELD_OFFSET);
        long lastAccessTime = decodeLong(initialFrame.content, LAST_ACCESS_TIME_FIELD_OFFSET);
        long lastUpdateTime = decodeLong(initialFrame.content, LAST_UPDATE_TIME_FIELD_OFFSET);
        long ttlMillis = decodeLong(initialFrame.content, TTL_MILLIS_FIELD_OFFSET);

        com.hazelcast.internal.serialization.Data key = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        com.hazelcast.internal.serialization.Data value = CodecUtil.decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder(key, value, creationTime, hits, lastAccessTime, lastUpdateTime, ttlMillis);
    }
}
