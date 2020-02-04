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

package com.hazelcast.log.encoders;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.util.UUID;

import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

public class UUIDEncoder implements Encoder<UUID> {
    private static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    @Override
    public int encode(long address, int available, UUID uuid) {
        int required = 2 * LONG_SIZE_IN_BYTES;

        if (available < required) {
            return -required;
        }

        UNSAFE.putLong(address, uuid.getMostSignificantBits());
        UNSAFE.putLong(address + LONG_SIZE_IN_BYTES, uuid.getLeastSignificantBits());

        return required;
    }

    @Override
    public UUID decode(long address, Consumed consumed) {
        long m = UNSAFE.getLong(address);
        long l = UNSAFE.getLong(address + LONG_SIZE_IN_BYTES);
        consumed.value = 2 * LONG_SIZE_IN_BYTES;
        return new UUID(m, l);
    }
}
