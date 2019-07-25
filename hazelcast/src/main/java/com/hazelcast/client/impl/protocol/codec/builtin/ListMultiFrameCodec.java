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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.NULL_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.nextFrameIsDataStructureEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.nextFrameIsNullEndFrame;

public class ListMultiFrameCodec {

    public static <T> void encode(ClientMessage clientMessage, Collection<T> collection,
            BiConsumer<ClientMessage, T> encodeFunction) {
        clientMessage.addFrame(BEGIN_FRAME);
        for (T item : collection) {
            encodeFunction.accept(clientMessage, item);
        }
        clientMessage.addFrame(END_FRAME);
    }

    public static <T> void encodeNullable(ClientMessage clientMessage, Collection<T> collection,
            BiConsumer<ClientMessage, T> encodeFunction) {
        if (collection == null) {
            clientMessage.addFrame(NULL_FRAME);
        } else {
            encode(clientMessage, collection, encodeFunction);
        }
    }

    public static <T> List<T> decode(ListIterator<ClientMessage.Frame> iterator,
            Function<ListIterator<ClientMessage.Frame>, T> decodeFunction) {
        List<T> result = new LinkedList<>();
        iterator.next();//begin frame, list
        while (!nextFrameIsDataStructureEndFrame(iterator)) {
            result.add(decodeFunction.apply(iterator));
        }
        iterator.next();//end frame, list
        return result;
    }

    public static <T> List<T> decodeNullable(ListIterator<ClientMessage.Frame> iterator,
            Function<ListIterator<ClientMessage.Frame>, T> decodeFunction) {
        return nextFrameIsNullEndFrame(iterator) ? null : decode(iterator, decodeFunction);
    }


}
