/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

@Generated("d94fc05e6b4bd1271d1acec8a946ad25")
public final class PagingPredicateHolderCodec {
    private static final int PAGE_SIZE_FIELD_OFFSET = 0;
    private static final int PAGE_FIELD_OFFSET = PAGE_SIZE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int ITERATION_TYPE_ID_FIELD_OFFSET = PAGE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = ITERATION_TYPE_ID_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private PagingPredicateHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.PagingPredicateHolder pagingPredicateHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, PAGE_SIZE_FIELD_OFFSET, pagingPredicateHolder.getPageSize());
        encodeInt(initialFrame.content, PAGE_FIELD_OFFSET, pagingPredicateHolder.getPage());
        encodeByte(initialFrame.content, ITERATION_TYPE_ID_FIELD_OFFSET, pagingPredicateHolder.getIterationTypeId());
        clientMessage.add(initialFrame);

        AnchorDataListHolderCodec.encode(clientMessage, pagingPredicateHolder.getAnchorDataListHolder());
        CodecUtil.encodeNullable(clientMessage, pagingPredicateHolder.getPredicateData(), DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, pagingPredicateHolder.getComparatorData(), DataCodec::encode);
        CodecUtil.encodeNullable(clientMessage, pagingPredicateHolder.getPartitionKeyData(), DataCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.PagingPredicateHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int pageSize = decodeInt(initialFrame.content, PAGE_SIZE_FIELD_OFFSET);
        int page = decodeInt(initialFrame.content, PAGE_FIELD_OFFSET);
        byte iterationTypeId = decodeByte(initialFrame.content, ITERATION_TYPE_ID_FIELD_OFFSET);

        com.hazelcast.client.impl.protocol.codec.holder.AnchorDataListHolder anchorDataListHolder = AnchorDataListHolderCodec.decode(iterator);
        com.hazelcast.internal.serialization.Data predicateData = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        com.hazelcast.internal.serialization.Data comparatorData = CodecUtil.decodeNullable(iterator, DataCodec::decode);
        com.hazelcast.internal.serialization.Data partitionKeyData = CodecUtil.decodeNullable(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.PagingPredicateHolder(anchorDataListHolder, predicateData, comparatorData, pageSize, page, iterationTypeId, partitionKeyData);
    }
}
