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

@Generated("a4f25df646c379cd7f770f7a08f34caf")
public final class SqlPageCodec {
    private static final int LAST_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = LAST_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private SqlPageCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.sql.impl.client.SqlPage sqlPage) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, LAST_FIELD_OFFSET, sqlPage.isLast());
        clientMessage.add(initialFrame);

        ListMultiFrameCodec.encode(clientMessage, sqlPage.getRows(), DataCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.sql.impl.client.SqlPage decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean last = decodeBoolean(initialFrame.content, LAST_FIELD_OFFSET);

        java.util.List<com.hazelcast.internal.serialization.Data> rows = ListMultiFrameCodec.decode(iterator, DataCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.sql.impl.client.SqlPage(rows, last);
    }
}
