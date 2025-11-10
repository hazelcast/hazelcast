/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.Data;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.hazelcast.client.impl.protocol.ClientMessage.NULL_FRAME;

public final class CodecUtil {

    private CodecUtil() {
    }

    public static void fastForwardToEndFrame(ClientMessage.ForwardFrameIterator iterator) {
        // We are starting from 1 because of the BEGIN_FRAME we read
        // in the beginning of the decode method
        int numberOfExpectedEndFrames = 1;
        ClientMessage.Frame frame;
        while (numberOfExpectedEndFrames != 0) {
            frame = iterator.next();
            if (frame.isEndFrame()) {
                numberOfExpectedEndFrames--;
            } else if (frame.isBeginFrame()) {
                numberOfExpectedEndFrames++;
            }
        }
    }

    public static <T> void encodeNullable(ClientMessage clientMessage, T value, BiConsumer<ClientMessage, T> encode) {
        assert !(value instanceof Data) : "For serializing nullable Data specialized codec must be used";

        if (value == null) {
            clientMessage.add(NULL_FRAME.copy());
        } else {
            encode.accept(clientMessage, value);
        }
    }

    public static <T> T decodeNullable(ClientMessage.ForwardFrameIterator iterator, Function<ClientMessage.ForwardFrameIterator, T> decode) {
        T result = nextFrameIsNullEndFrame(iterator) ? null : decode.apply(iterator);
        assert !(result instanceof Data) : "For deserializing nullable Data specialized codec must be used";
        return result;
    }

    public static boolean nextFrameIsDataStructureEndFrame(ClientMessage.ForwardFrameIterator iterator) {
        return iterator.peekNext().isEndFrame();
    }

    public static boolean nextFrameIsNullEndFrame(ClientMessage.ForwardFrameIterator iterator) {
        boolean isNull = iterator.peekNext().isNullFrame();
        if (isNull) {
            iterator.next();
        }
        return isNull;
    }
}
