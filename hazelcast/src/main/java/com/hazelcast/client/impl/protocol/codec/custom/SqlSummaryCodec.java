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
@Generated("5c1c05a1b7ea924e049e84eaf77a2625")
public final class SqlSummaryCodec {
    private static final int UNBOUNDED_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = UNBOUNDED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;

    private SqlSummaryCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.jet.impl.SqlSummary sqlSummary) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, UNBOUNDED_FIELD_OFFSET, sqlSummary.isUnbounded());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, sqlSummary.getQuery());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.jet.impl.SqlSummary decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean unbounded = decodeBoolean(initialFrame.content, UNBOUNDED_FIELD_OFFSET);

        java.lang.String query = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createSqlSummary(query, unbounded);
    }
}
