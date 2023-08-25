/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.expression.RowValue;

public final class HazelcastRowValueCodec {
    private HazelcastRowValueCodec() { }

    public static void encode(ClientMessage clientMessage, RowValue rowValue) {
        StringCodec.encode(clientMessage, rowValue.toString());
    }

    public static String decode(ClientMessage.ForwardFrameIterator iterator) {
        return StringCodec.decode(iterator);
    }
}
