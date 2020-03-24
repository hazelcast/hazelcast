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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.LONG_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.UUID_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeUUID;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeUUID;

public final class EntryListUUIDLongCodec {

    private static final int ENTRY_SIZE_IN_BYTES = UUID_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES;

    private EntryListUUIDLongCodec() {
    }

    public static void encode(ClientMessage clientMessage, Collection<Map.Entry<UUID, Long>> collection) {
        int itemCount = collection.size();
        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[itemCount * ENTRY_SIZE_IN_BYTES]);
        Iterator<Map.Entry<UUID, Long>> iterator = collection.iterator();
        for (int i = 0; i < itemCount; i++) {
            Map.Entry<UUID, Long> entry = iterator.next();
            encodeUUID(frame.content, i * ENTRY_SIZE_IN_BYTES, entry.getKey());
            encodeLong(frame.content, i * ENTRY_SIZE_IN_BYTES + UUID_SIZE_IN_BYTES, entry.getValue());
        }
        clientMessage.add(frame);
    }

    public static List<Map.Entry<UUID, Long>> decode(ClientMessage.ForwardFrameIterator iterator) {
        ClientMessage.Frame frame = iterator.next();
        int itemCount = frame.content.length / ENTRY_SIZE_IN_BYTES;
        List<Map.Entry<UUID, Long>> result = new ArrayList<>(itemCount);
        for (int i = 0; i < itemCount; i++) {
            UUID key = decodeUUID(frame.content, i * ENTRY_SIZE_IN_BYTES);
            Long value = decodeLong(frame.content, i * ENTRY_SIZE_IN_BYTES + UUID_SIZE_IN_BYTES);
            result.add(new AbstractMap.SimpleEntry<>(key, value));
        }
        return result;
    }
}
