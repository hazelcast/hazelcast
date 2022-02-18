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

@Generated("5a72fe60a2ad9992c5207539d01ebe99")
public final class MCEventCodec {
    private static final int TIMESTAMP_FIELD_OFFSET = 0;
    private static final int TYPE_FIELD_OFFSET = TIMESTAMP_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = TYPE_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private MCEventCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.internal.management.dto.MCEventDTO mCEvent) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, TIMESTAMP_FIELD_OFFSET, mCEvent.getTimestamp());
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, mCEvent.getType());
        clientMessage.add(initialFrame);

        StringCodec.encode(clientMessage, mCEvent.getDataJson());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.internal.management.dto.MCEventDTO decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long timestamp = decodeLong(initialFrame.content, TIMESTAMP_FIELD_OFFSET);
        int type = decodeInt(initialFrame.content, TYPE_FIELD_OFFSET);

        java.lang.String dataJson = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.internal.management.dto.MCEventDTO(timestamp, type, dataJson);
    }
}
