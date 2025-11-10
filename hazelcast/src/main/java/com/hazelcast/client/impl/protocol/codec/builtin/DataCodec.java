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
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationUtil;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.hazelcast.client.impl.protocol.ClientMessage.NULL_FRAME;
import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.nextFrameIsNullEndFrame;

public final class DataCodec {

    /**
     * Disables canonicalization of non-canonical {@link Data} representations of {@code null}
     * - if such non-canonical from is received it will be kept as is during client message serde
     * instead of being interpreted as actual {@code null}.
     * <p>
     * This flag is provided as last resort for compatibility with users that rely on such undocumented behavior.
     * It should be noted that enabling old behavior introduces some security and stability risks.
     */
    public static final String PROPERTY_KEEP_NON_CANONICAL_NULL = "hazelcast.serialization.keep.noncanonical.null";
    private static final boolean KEEP_NON_CANONICAL_NULL = Boolean.getBoolean(PROPERTY_KEEP_NON_CANONICAL_NULL);

    private DataCodec() {
    }

    public static void encode(ClientMessage clientMessage, Data data) {
        if (SerializationUtil.isNullData(data) && !KEEP_NON_CANONICAL_NULL) {
            throw new IllegalArgumentException("Non-null Data field cannot be sent with null value");
        }
        clientMessage.add(new ClientMessage.Frame(data.toByteArray()));
    }

    /**
     * @apiNote This method is not equivalent to {@link CodecUtil#encodeNullable(ClientMessage, Object, BiConsumer)}.
     * With {@link Data} this method should be used instead of generic one from {@link CodecUtil}.
     */
    public static void encodeNullable(ClientMessage clientMessage, Data data) {
        if (data == null || (SerializationUtil.isNullData(data) && !KEEP_NON_CANONICAL_NULL)) {
            clientMessage.add(NULL_FRAME.copy());
        } else {
            clientMessage.add(new ClientMessage.Frame(data.toByteArray()));
        }
    }

    public static Data decode(ClientMessage.ForwardFrameIterator iterator) {
        HeapData heapData = new HeapData(iterator.next().content);
        if (SerializationUtil.isNullData(heapData) && !KEEP_NON_CANONICAL_NULL) {
            throw new IllegalArgumentException("Non-null Data field with null value in incoming message");
        }
        return heapData;
    }

    /**
     * @apiNote This method is not equivalent to {@link CodecUtil#decodeNullable(ClientMessage.ForwardFrameIterator, Function)}.
     * With {@link Data} this method should be used instead of generic one from {@link CodecUtil}.
     */
    public static Data decodeNullable(ClientMessage.ForwardFrameIterator iterator) {
        if (nextFrameIsNullEndFrame(iterator)) {
            return null;
        }
        HeapData heapData = new HeapData(iterator.next().content);
        return SerializationUtil.isNullData(heapData) && !KEEP_NON_CANONICAL_NULL ? null : heapData;
    }
}
