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

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;

public class ByteArrayEncoder implements Encoder<byte[]> {
    public static final ByteArrayEncoder INSTANCE = new ByteArrayEncoder();

    private final static Unsafe UNSAFE = UnsafeUtil.UNSAFE;
    private final static long BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    @Override
    public int encode(long address, int available, byte[] b) {
        int required = b.length + INT_SIZE_IN_BYTES;
        if (available < required) {
            return -required;
        }

        UNSAFE.putInt(address, b.length);
        address += INT_SIZE_IN_BYTES;

        UNSAFE.copyMemory(b, BASE_OFFSET, null, address, b.length);
        return required;
    }

    @Override
    public byte[] decode(long address, Consumed consumed) {
        int length = UNSAFE.getInt(address);
        byte[] bytes = new byte[length];
        address += INT_SIZE_IN_BYTES;

        consumed.value = INT_SIZE_IN_BYTES + length;
        UNSAFE.copyMemory(null, address, bytes, BASE_OFFSET, length);
        return bytes;
    }
}
