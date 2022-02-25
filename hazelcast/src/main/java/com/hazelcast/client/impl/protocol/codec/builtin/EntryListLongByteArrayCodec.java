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

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;

public final class EntryListLongByteArrayCodec {

    private EntryListLongByteArrayCodec() {
    }

    public static void encode(ClientMessage clientMessage, Collection<Map.Entry<Long, byte[]>> collection) {
        List<Long> valueList = new ArrayList<>(collection.size());
        clientMessage.add(BEGIN_FRAME.copy());
        for (Map.Entry<Long, byte[]> entry : collection) {
            valueList.add(entry.getKey());
            ByteArrayCodec.encode(clientMessage, entry.getValue());
        }
        clientMessage.add(END_FRAME.copy());
        ListLongCodec.encode(clientMessage, valueList);
    }

    public static List<Map.Entry<Long, byte[]>> decode(ClientMessage.ForwardFrameIterator iterator) {
        List<byte[]> listV = ListMultiFrameCodec.decode(iterator, ByteArrayCodec::decode);
        List<Long> listK = ListLongCodec.decode(iterator);

        List<Map.Entry<Long, byte[]>> result = new ArrayList<>(listK.size());
        for (int i = 0; i < listK.size(); i++) {
            result.add(new AbstractMap.SimpleEntry<>(listK.get(i), listV.get(i)));
        }
        return result;
    }
}
