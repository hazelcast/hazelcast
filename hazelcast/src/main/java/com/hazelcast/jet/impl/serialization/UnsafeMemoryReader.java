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

package com.hazelcast.jet.impl.serialization;

import java.nio.ByteOrder;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM_AVAILABLE;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static java.nio.ByteOrder.nativeOrder;

public class UnsafeMemoryReader implements MemoryReader {

    private final boolean reverse;

    UnsafeMemoryReader(ByteOrder byteOrder) {
        checkState(MEM_AVAILABLE, "unsafe memory access is not available");
        this.reverse = nativeOrder() != byteOrder;
    }

    @Override
    public int readInt(byte[] bytes, int offset) {
        int value = MEM.getInt(bytes, ARRAY_BYTE_BASE_OFFSET + offset);
        return reverse ? Integer.reverseBytes(value) : value;
    }

    @Override
    public long readLong(byte[] bytes, int offset) {
        long value = MEM.getLong(bytes, ARRAY_BYTE_BASE_OFFSET + offset);
        return reverse ? Long.reverseBytes(value) : value;
    }
}
