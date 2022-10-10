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

package com.hazelcast.internal.alto.offheapmap;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import static com.hazelcast.internal.nio.Bits.BYTES_INT;

public class ExampleQuery implements Query {
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;

    public long result;

    @Override
    public void clear() {
        result = 0;
    }

    @Override
    public void process(long address) {
        long keyLength = unsafe.getInt(address);
        long keyValueAddress = address + BYTES_INT;
        long valueLength = unsafe.getInt(address + BYTES_INT + keyLength);
        long valueAddress = unsafe.getInt(address + BYTES_INT + keyLength + valueLength);

        result += valueLength + keyLength;
    }
}
