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

@Generated("18d8322d3f88372e889b0f2e9fc6c8b9")
public final class SqlColumnMetadataCodec {
    private static final int TYPE_FIELD_OFFSET = 0;
    private static final int NULLABLE_FIELD_OFFSET = TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = NULLABLE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private SqlColumnMetadataCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.sql.SqlColumnMetadata sqlColumnMetadata) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, sqlColumnMetadata.getType());
        encodeBoolean(initialFrame.content, NULLABLE_FIELD_OFFSET, sqlColumnMetadata.isNullable());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, sqlColumnMetadata.getName());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.sql.SqlColumnMetadata decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int type = decodeInt(initialFrame.content, TYPE_FIELD_OFFSET);
        boolean isNullableExists = false;
        boolean nullable = false;
        if (initialFrame.content.length >= NULLABLE_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES) {
            nullable = decodeBoolean(initialFrame.content, NULLABLE_FIELD_OFFSET);
            isNullableExists = true;
        }

        java.lang.String name = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createSqlColumnMetadata(name, type, isNullableExists, nullable);
    }
}
