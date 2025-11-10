/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
@Generated("9342e984aa50f0c272c45707e67d0bd0")
public final class VectorSearchResultCodec {
    private static final int SCORE_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = SCORE_FIELD_OFFSET + FLOAT_SIZE_IN_BYTES;

    private VectorSearchResultCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.vector.SearchResult<com.hazelcast.internal.serialization.Data, com.hazelcast.internal.serialization.Data> vectorSearchResult) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeFloat(initialFrame.content, SCORE_FIELD_OFFSET, vectorSearchResult.getScore());
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, vectorSearchResult.getKey());
        DataCodec.encodeNullable(clientMessage, vectorSearchResult.getValue());
        ListMultiFrameCodec.encodeNullable(clientMessage, vectorSearchResult.getVectors(), VectorPairCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.vector.impl.DataSearchResult decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        float score = decodeFloat(initialFrame.content, SCORE_FIELD_OFFSET);

        com.hazelcast.internal.serialization.Data key = DataCodec.decode(iterator);
        com.hazelcast.internal.serialization.Data value = DataCodec.decodeNullable(iterator);
        java.util.List<com.hazelcast.client.impl.protocol.codec.holder.VectorPairHolder> vectors = ListMultiFrameCodec.decodeNullable(iterator, VectorPairCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createVectorSearchResult(key, value, score, vectors);
    }
}
