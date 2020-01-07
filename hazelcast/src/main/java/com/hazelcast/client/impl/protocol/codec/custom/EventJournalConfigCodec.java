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

@Generated("9a431032d3e3fe2da1d30d57cb6bf53d")
public final class EventJournalConfigCodec {
    private static final int ENABLED_FIELD_OFFSET = 0;
    private static final int CAPACITY_FIELD_OFFSET = ENABLED_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int TIME_TO_LIVE_SECONDS_FIELD_OFFSET = CAPACITY_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = TIME_TO_LIVE_SECONDS_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private EventJournalConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.config.EventJournalConfig eventJournalConfig) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET, eventJournalConfig.isEnabled());
        encodeInt(initialFrame.content, CAPACITY_FIELD_OFFSET, eventJournalConfig.getCapacity());
        encodeInt(initialFrame.content, TIME_TO_LIVE_SECONDS_FIELD_OFFSET, eventJournalConfig.getTimeToLiveSeconds());
        clientMessage.add(initialFrame);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.config.EventJournalConfig decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = decodeBoolean(initialFrame.content, ENABLED_FIELD_OFFSET);
        int capacity = decodeInt(initialFrame.content, CAPACITY_FIELD_OFFSET);
        int timeToLiveSeconds = decodeInt(initialFrame.content, TIME_TO_LIVE_SECONDS_FIELD_OFFSET);

        fastForwardToEndFrame(iterator);

        return CustomTypeFactory.createEventJournalConfig(enabled, capacity, timeToLiveSeconds);
    }
}
