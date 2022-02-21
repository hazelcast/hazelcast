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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;

public final class EntryListUUIDListIntegerCodec {

    private EntryListUUIDListIntegerCodec() {
    }

    public static void encode(ClientMessage clientMessage, Collection<Map.Entry<UUID, List<Integer>>> collection) {
        List<UUID> keyList = new ArrayList<>(collection.size());
        clientMessage.add(BEGIN_FRAME.copy());
        for (Map.Entry<UUID, List<Integer>> entry : collection) {
            keyList.add(entry.getKey());
            ListIntegerCodec.encode(clientMessage, entry.getValue());
        }
        clientMessage.add(END_FRAME.copy());
        ListUUIDCodec.encode(clientMessage, keyList);
    }

    public static Collection<Map.Entry<UUID, List<Integer>>> decode(ClientMessage.ForwardFrameIterator iterator) {
        List<List<Integer>> listv = ListMultiFrameCodec.decode(iterator, ListIntegerCodec::decode);
        List<UUID> listK = ListUUIDCodec.decode(iterator);

        List<Map.Entry<UUID, List<Integer>>> result = new ArrayList<>(listK.size());
        for (int i = 0; i < listK.size(); i++) {
            result.add(new AbstractMap.SimpleEntry<>(listK.get(i), listv.get(i)));
        }
        return result;
    }

}
