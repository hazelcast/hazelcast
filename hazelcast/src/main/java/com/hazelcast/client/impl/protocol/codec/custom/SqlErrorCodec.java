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

@Generated("1098dbc5f24f045826ae422c5c9b7ac7")
public final class SqlErrorCodec {
    private static final int CODE_FIELD_OFFSET = 0;
    private static final int ORIGINATING_MEMBER_ID_FIELD_OFFSET = CODE_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = ORIGINATING_MEMBER_ID_FIELD_OFFSET + UUID_SIZE_IN_BYTES;

    private SqlErrorCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.sql.impl.client.SqlError sqlError) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, CODE_FIELD_OFFSET, sqlError.getCode());
        encodeUUID(initialFrame.content, ORIGINATING_MEMBER_ID_FIELD_OFFSET, sqlError.getOriginatingMemberId());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, sqlError.getMessage(), StringCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.sql.impl.client.SqlError decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int code = decodeInt(initialFrame.content, CODE_FIELD_OFFSET);
        java.util.UUID originatingMemberId = decodeUUID(initialFrame.content, ORIGINATING_MEMBER_ID_FIELD_OFFSET);

        java.lang.String message = CodecUtil.decodeNullable(iterator, StringCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.sql.impl.client.SqlError(code, message, originatingMemberId);
    }
}
