/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.internal.nio.Bits;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;

public final class EventJournalConfigCodec {

    private static final int ENABLED_OFFSET = 0;
    private static final int CAPACITY_OFFSET = ENABLED_OFFSET + Bits.BOOLEAN_SIZE_IN_BYTES;
    private static final int TTL_OFFSET = CAPACITY_OFFSET + Bits.INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = TTL_OFFSET + Bits.INT_SIZE_IN_BYTES;

    private EventJournalConfigCodec() {
    }

    public static void encode(ClientMessage clientMessage, EventJournalConfig eventJournalConfig) {
        clientMessage.add(BEGIN_FRAME);

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeBoolean(initialFrame.content, ENABLED_OFFSET, eventJournalConfig.isEnabled());
        encodeInt(initialFrame.content, CAPACITY_OFFSET, eventJournalConfig.getCapacity());
        encodeInt(initialFrame.content, TTL_OFFSET, eventJournalConfig.getTimeToLiveSeconds());
        clientMessage.add(initialFrame);

        clientMessage.add(END_FRAME);

    }

    public static EventJournalConfig decode(ListIterator<ClientMessage.Frame> iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        boolean enabled = decodeBoolean(initialFrame.content, ENABLED_OFFSET);
        int capacity = decodeInt(initialFrame.content, CAPACITY_OFFSET);
        int ttl = decodeInt(initialFrame.content, TTL_OFFSET);

        fastForwardToEndFrame(iterator);

        EventJournalConfig eventJournalConfig = new EventJournalConfig();
        eventJournalConfig.setEnabled(enabled);
        eventJournalConfig.setCapacity(capacity);
        eventJournalConfig.setTimeToLiveSeconds(ttl);
        return eventJournalConfig;
    }
}
