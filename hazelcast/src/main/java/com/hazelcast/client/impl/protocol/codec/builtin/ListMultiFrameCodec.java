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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAME;
import static com.hazelcast.client.impl.protocol.ClientMessage.NULL_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.nextFrameIsDataStructureEndFrame;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.nextFrameIsNullEndFrame;

public final class ListMultiFrameCodec {

    private ListMultiFrameCodec() {
    }

    public static <T> void encode(ClientMessage clientMessage, Iterable<T> collection,
                                  BiConsumer<ClientMessage, T> encodeFunction) {
        clientMessage.add(BEGIN_FRAME.copy());
        for (T item : collection) {
            encodeFunction.accept(clientMessage, item);
        }
        clientMessage.add(END_FRAME.copy());
    }

    public static <T> void encodeContainsNullable(ClientMessage clientMessage, Iterable<T> collection,
                                                  BiConsumer<ClientMessage, T> encodeFunction) {
        clientMessage.add(BEGIN_FRAME.copy());
        for (T item : collection) {
            if (item == null) {
                clientMessage.add(NULL_FRAME.copy());
            } else {
                encodeFunction.accept(clientMessage, item);
            }
        }
        clientMessage.add(END_FRAME.copy());
    }

    public static <T> void encodeNullable(ClientMessage clientMessage, Collection<T> collection,
                                          BiConsumer<ClientMessage, T> encodeFunction) {
        if (collection == null) {
            clientMessage.add(NULL_FRAME.copy());
        } else {
            encode(clientMessage, collection, encodeFunction);
        }
    }

    public static <T> List<T> decode(ClientMessage.ForwardFrameIterator iterator,
                                     Function<ClientMessage.ForwardFrameIterator, T> decodeFunction) {
        List<T> result = new ArrayList<>();
        //begin frame, list
        iterator.next();
        while (!nextFrameIsDataStructureEndFrame(iterator)) {
            result.add(decodeFunction.apply(iterator));
        }
        //end frame, list
        iterator.next();
        return result;
    }

    public static <T> List<T> decodeContainsNullable(ClientMessage.ForwardFrameIterator iterator,
                                                     Function<ClientMessage.ForwardFrameIterator, T> decodeFunction) {
        List<T> result = new ArrayList<>();
        //begin frame, list
        iterator.next();
        while (!nextFrameIsDataStructureEndFrame(iterator)) {
            result.add(nextFrameIsNullEndFrame(iterator) ? null : decodeFunction.apply(iterator));
        }
        //end frame, list
        iterator.next();
        return result;
    }

    public static <T> List<T> decodeNullable(ClientMessage.ForwardFrameIterator iterator,
                                             Function<ClientMessage.ForwardFrameIterator, T> decodeFunction) {
        return nextFrameIsNullEndFrame(iterator) ? null : decode(iterator, decodeFunction);
    }
}
