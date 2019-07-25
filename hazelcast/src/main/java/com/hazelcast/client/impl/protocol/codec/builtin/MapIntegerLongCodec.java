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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;

public class MapIntegerLongCodec {
    public static void encode(ClientMessage clientMessage, Collection<Map.Entry<Integer, Long>> collection) {
        List<Integer> keyList = new ArrayList<>(collection.size());
        List<Long> valueList = new ArrayList<>(collection.size());
        for (Map.Entry<Integer, Long> entry : collection) {
            keyList.add(entry.getKey());
            valueList.add(entry.getValue());
        }
        ListIntegerCodec.encode(clientMessage, keyList);
        ListLongCodec.encode(clientMessage, valueList);
    }

    public static List<Map.Entry<Integer, Long>> decode(ListIterator<ClientMessage.Frame> iterator) {
        List<Integer> keyList = ListIntegerCodec.decode(iterator);
        List<Long> valueList = ListLongCodec.decode(iterator);
        int mapSize = keyList.size();
        List<Map.Entry<Integer, Long>> result = new ArrayList<>(mapSize);
        for (int i = 0; i < mapSize; i++){
            result.add(new AbstractMap.SimpleEntry<>(keyList.get(i), valueList.get(i)));
        }
        return result;
    }
}
