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
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.client.impl.protocol.ClientMessage.NULL_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.nextFrameIsNullEndFrame;

public final class DataCodec {

    private DataCodec() {
    }

    public static void encode(ClientMessage clientMessage, Data data) {
        clientMessage.add(new ClientMessage.Frame(data.toByteArray()));
    }

    public static void encodeNullable(ClientMessage clientMessage, Data data) {
        if (data == null) {
            clientMessage.add(NULL_FRAME.copy());
        } else {
            clientMessage.add(new ClientMessage.Frame(data.toByteArray()));
        }
    }

    public static Data decode(ClientMessage.ForwardFrameIterator iterator) {
        return new HeapData(iterator.next().content);
    }

    public static Data decodeNullable(ClientMessage.ForwardFrameIterator iterator) {
        return nextFrameIsNullEndFrame(iterator) ? null : decode(iterator);
    }

}
