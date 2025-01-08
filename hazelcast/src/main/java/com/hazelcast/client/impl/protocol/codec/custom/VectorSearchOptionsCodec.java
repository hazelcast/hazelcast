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
@Generated("dac190fc9657335720b0d26318065624")
public final class VectorSearchOptionsCodec {
    private static final int INCLUDE_VALUE_FIELD_OFFSET = 0;
    private static final int INCLUDE_VECTORS_FIELD_OFFSET = INCLUDE_VALUE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int LIMIT_FIELD_OFFSET = INCLUDE_VECTORS_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = LIMIT_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private VectorSearchOptionsCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.vector.SearchOptions vectorSearchOptions) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, INCLUDE_VALUE_FIELD_OFFSET, vectorSearchOptions.isIncludeValue());
        encodeBoolean(initialFrame.content, INCLUDE_VECTORS_FIELD_OFFSET, vectorSearchOptions.isIncludeVectors());
        encodeInt(initialFrame.content, LIMIT_FIELD_OFFSET, vectorSearchOptions.getLimit());
        clientMessage.add(initialFrame);

        MapCodec.encodeNullable(clientMessage, vectorSearchOptions.getHints(), StringCodec::encode, StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.vector.SearchOptions decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean includeValue = decodeBoolean(initialFrame.content, INCLUDE_VALUE_FIELD_OFFSET);
        boolean includeVectors = decodeBoolean(initialFrame.content, INCLUDE_VECTORS_FIELD_OFFSET);
        int limit = decodeInt(initialFrame.content, LIMIT_FIELD_OFFSET);

        java.util.Map<java.lang.String, java.lang.String> hints = MapCodec.decodeNullable(iterator, StringCodec::decode, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createVectorSearchOptions(includeValue, includeVectors, limit, hints);
    }
}
