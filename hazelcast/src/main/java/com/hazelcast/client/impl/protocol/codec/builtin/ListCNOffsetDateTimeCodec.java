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

import java.time.OffsetDateTime;
import java.util.List;

import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.OFFSET_DATE_TIME_SIZE_IN_BYTES;

public final class ListCNOffsetDateTimeCodec {

    private ListCNOffsetDateTimeCodec() {
    }

    public static void encode(ClientMessage clientMessage, Iterable<OffsetDateTime> items) {
        ListCNFixedSizeCodec.encode(
            clientMessage, items, OFFSET_DATE_TIME_SIZE_IN_BYTES, FixedSizeTypesCodec::encodeOffsetDateTime
        );
    }

    public static List<OffsetDateTime> decode(ClientMessage.ForwardFrameIterator iterator) {
        return decode(iterator.next());
    }

    public static List<OffsetDateTime> decode(ClientMessage.Frame frame) {
        return ListCNFixedSizeCodec.decode(frame, OFFSET_DATE_TIME_SIZE_IN_BYTES, FixedSizeTypesCodec::decodeOffsetDateTime);
    }
}
