/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.annotation.Codec;
import com.hazelcast.annotation.Since;
import com.hazelcast.client.impl.protocol.ClientMessage;

import java.util.UUID;

import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;

@Since("1.4")
@Codec(UUID.class)
public final class UUIDCodec {

    private static final int UUID_LONG_FIELD_COUNT = 2;
    private static final int UUID_DATA_SIZE = UUID_LONG_FIELD_COUNT * LONG_SIZE_IN_BYTES;

    private UUIDCodec() {
    }

    public static UUID decode(ClientMessage clientMessage) {
        return new UUID(clientMessage.getLong(), clientMessage.getLong());
    }

    public static void encode(UUID uuid, ClientMessage clientMessage) {
        clientMessage.set(uuid.getMostSignificantBits());
        clientMessage.set(uuid.getLeastSignificantBits());
    }

    public static int calculateDataSize(UUID uuid) {
        return UUID_DATA_SIZE;
    }
}
