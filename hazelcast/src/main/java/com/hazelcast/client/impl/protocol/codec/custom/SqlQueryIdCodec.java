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

@Generated("4c1dfd0c2849352e25d8479bbf05001a")
public final class SqlQueryIdCodec {
    private static final int MEMBER_ID_HIGH_FIELD_OFFSET = 0;
    private static final int MEMBER_ID_LOW_FIELD_OFFSET = MEMBER_ID_HIGH_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int LOCAL_ID_HIGH_FIELD_OFFSET = MEMBER_ID_LOW_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int LOCAL_ID_LOW_FIELD_OFFSET = LOCAL_ID_HIGH_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = LOCAL_ID_LOW_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private SqlQueryIdCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.sql.impl.QueryId sqlQueryId) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, MEMBER_ID_HIGH_FIELD_OFFSET, sqlQueryId.getMemberIdHigh());
        encodeLong(initialFrame.content, MEMBER_ID_LOW_FIELD_OFFSET, sqlQueryId.getMemberIdLow());
        encodeLong(initialFrame.content, LOCAL_ID_HIGH_FIELD_OFFSET, sqlQueryId.getLocalIdHigh());
        encodeLong(initialFrame.content, LOCAL_ID_LOW_FIELD_OFFSET, sqlQueryId.getLocalIdLow());
        clientMessage.add(initialFrame);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.sql.impl.QueryId decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long memberIdHigh = decodeLong(initialFrame.content, MEMBER_ID_HIGH_FIELD_OFFSET);
        long memberIdLow = decodeLong(initialFrame.content, MEMBER_ID_LOW_FIELD_OFFSET);
        long localIdHigh = decodeLong(initialFrame.content, LOCAL_ID_HIGH_FIELD_OFFSET);
        long localIdLow = decodeLong(initialFrame.content, LOCAL_ID_LOW_FIELD_OFFSET);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.sql.impl.QueryId(memberIdHigh, memberIdLow, localIdHigh, localIdLow);
    }
}
