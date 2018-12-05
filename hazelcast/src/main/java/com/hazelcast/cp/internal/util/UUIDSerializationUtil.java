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

package com.hazelcast.cp.internal.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;

/**
 * Util methods for UUID serialization / deserialization operations
 */
public final class UUIDSerializationUtil {

    private UUIDSerializationUtil() {
    }

    public static void writeUUID(ObjectDataOutput out, UUID uid) throws IOException {
        out.writeLong(uid.getLeastSignificantBits());
        out.writeLong(uid.getMostSignificantBits());
    }

    public static UUID readUUID(ObjectDataInput in) throws IOException {
        long least = in.readLong();
        long most = in.readLong();

        return new UUID(most, least);
    }

    public static UUID readUUID(ClientMessage msg) {
        long least = msg.getLong();
        long most = msg.getLong();

        return new UUID(most, least);
    }
}
