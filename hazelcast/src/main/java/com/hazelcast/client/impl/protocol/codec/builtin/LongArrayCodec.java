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

import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.LONG_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;

public final class LongArrayCodec {

    private LongArrayCodec() {
    }

    public static void encode(ClientMessage clientMessage, long[] array) {
        int itemCount = array.length;
        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[itemCount * LONG_SIZE_IN_BYTES]);
        for (int i = 0; i < itemCount; i++) {
            encodeLong(frame.content, i * LONG_SIZE_IN_BYTES, array[i]);
        }
        clientMessage.add(frame);
    }

    public static long[] decode(ClientMessage.Frame frame) {
        int itemCount = frame.content.length / LONG_SIZE_IN_BYTES;
        long[] result = new long[itemCount];
        for (int i = 0; i < itemCount; i++) {
            result[i] = decodeLong(frame.content, i * LONG_SIZE_IN_BYTES);
        }
        return result;
    }

    public static long[] decode(ClientMessage.ForwardFrameIterator iterator) {
        return decode(iterator.next());
    }

}
