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
import com.hazelcast.map.impl.querycache.event.DefaultQueryCacheEventData;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;

public final class QueryCacheEventDataCodec {

    private static final int SEQUENCE_OFFSET = 0;
    private static final int EVENT_TYPE_OFFSET = SEQUENCE_OFFSET + Bits.LONG_SIZE_IN_BYTES;
    private static final int PARTITION_ID_OFFSET = EVENT_TYPE_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = PARTITION_ID_OFFSET + Bits.INT_SIZE_IN_BYTES;

    private QueryCacheEventDataCodec() {
    }

    public static void encode(ClientMessage clientMessage, QueryCacheEventData eventData) {
        clientMessage.addFrame(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        FixedSizeTypesCodec.encodeLong(initialFrame.content, SEQUENCE_OFFSET, eventData.getSequence());
        FixedSizeTypesCodec.encodeInt(initialFrame.content, EVENT_TYPE_OFFSET, eventData.getEventType());
        FixedSizeTypesCodec.encodeInt(initialFrame.content, PARTITION_ID_OFFSET, eventData.getPartitionId());
        clientMessage.addFrame(initialFrame);

        CodecUtil.encodeNullable(clientMessage, eventData.getDataKey(), DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, eventData.getDataNewValue(), DataCodec::encode);

        clientMessage.addFrame(END_FRAME);
    }

    public static QueryCacheEventData decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long sequence = FixedSizeTypesCodec.decodeLong(initialFrame.content, SEQUENCE_OFFSET);
        int eventType = FixedSizeTypesCodec.decodeInt(initialFrame.content, EVENT_TYPE_OFFSET);
        int partitionId = FixedSizeTypesCodec.decodeInt(initialFrame.content, PARTITION_ID_OFFSET);

        Data key = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        Data newValue = CodecUtil.decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        DefaultQueryCacheEventData queryCacheEventData = new DefaultQueryCacheEventData();
        queryCacheEventData.setSequence(sequence);
        queryCacheEventData.setEventType(eventType);
        queryCacheEventData.setPartitionId(partitionId);
        queryCacheEventData.setDataKey(key);
        queryCacheEventData.setDataNewValue(newValue);
        return queryCacheEventData;
    }
}
