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

import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.INT_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.LONG_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInteger;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInteger;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;

public final class EntryListIntegerLongCodec {

    private static final int ENTRY_SIZE_IN_BYTES = INT_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES;

    private EntryListIntegerLongCodec() {
    }

    public static void encode(ClientMessage clientMessage, Collection<Map.Entry<Integer, Long>> collection) {
        int itemCount = collection.size();
        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[itemCount * ENTRY_SIZE_IN_BYTES]);
        Iterator<Map.Entry<Integer, Long>> iterator = collection.iterator();
        for (int i = 0; i < itemCount; i++) {
            Map.Entry<Integer, Long> entry = iterator.next();
            encodeInteger(frame.content, i * ENTRY_SIZE_IN_BYTES, entry.getKey());
            encodeLong(frame.content, i * ENTRY_SIZE_IN_BYTES + INT_SIZE_IN_BYTES, entry.getValue());
        }
        clientMessage.add(frame);
    }

    public static List<Map.Entry<Integer, Long>> decode(ClientMessage.ForwardFrameIterator iterator) {
        ClientMessage.Frame frame = iterator.next();
        int itemCount = frame.content.length / ENTRY_SIZE_IN_BYTES;
        List<Map.Entry<Integer, Long>> result = new ArrayList<>(itemCount);
        for (int i = 0; i < itemCount; i++) {
            int key = decodeInteger(frame.content, i * ENTRY_SIZE_IN_BYTES);
            long value = decodeLong(frame.content, i * ENTRY_SIZE_IN_BYTES + INT_SIZE_IN_BYTES);
            result.add(new AbstractMap.SimpleEntry<>(key, value));
        }
        return result;
    }
}
