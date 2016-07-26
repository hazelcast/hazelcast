/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.util;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.memory.serialization.MemoryDataInput;
import com.hazelcast.jet.memory.serialization.MemoryDataOutput;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.nio.Bits;

import java.io.IOException;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.jet.memory.util.Util.BYTE_0;

/**
 * Utility methods related to I/O
 */
public final class JetIoUtil {
    public static final int KEY_BLOCK_OFFSET = 8;

    private JetIoUtil() {
    }

    public static long addressOfKeyBlockAt(long tupleAddress) {
        return tupleAddress + KEY_BLOCK_OFFSET;
    }

    public static long sizeOfKeyBlockAt(long tupleAddress, MemoryAccessor memoryAccessor) {
        return getLong(tupleAddress, 0, memoryAccessor);
    }

    public static long addrOfValueBlockAt(long tupleAddress, MemoryAccessor memoryAccessor) {
        long keySize = sizeOfKeyBlockAt(tupleAddress, memoryAccessor);
        return tupleAddress + offsetOfValueSizeField(keySize) + Bits.LONG_SIZE_IN_BYTES;
    }

    public static long sizeOfValueBlockAt(long tupleAddress, MemoryAccessor memoryAccessor) {
        long keySize = sizeOfKeyBlockAt(tupleAddress, memoryAccessor);
        return getLong(tupleAddress, offsetOfValueSizeField(keySize), memoryAccessor);
    }

    public static long sizeOfTupleAt(long tupleAddress, MemoryAccessor memoryAccessor) {
        final long keySize = getLong(tupleAddress, 0, memoryAccessor);
        final long valueSize = getLong(tupleAddress, offsetOfValueSizeField(keySize), memoryAccessor);
        return Bits.LONG_SIZE_IN_BYTES + keySize + Bits.LONG_SIZE_IN_BYTES + valueSize;
    }

    public static void writeTuple(Pair pair, MemoryDataOutput output, MemoryManager memoryManager) {
        output.clear();
        output.setMemoryManager(memoryManager);
        try {
            for (int i = 0; i < 2; i++) {
                // Remember position of the size field
                final long initialPos = output.position();
                // Reserve space for the size field before its value is known
                output.skip(Bits.LONG_SIZE_IN_BYTES);
                // Remember initial block size, to calculate the size of what this iteration wrote
                final long initialSize = output.usedSize();
                output.writeOptimized(pair.get(i));
                memoryManager.getAccessor().putLong(output.baseAddress() + initialPos, output.usedSize() - initialSize);
            }
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }

    public static void readTuple(
            MemoryDataInput input, long tupleAddress, Pair pair, MemoryAccessor memoryAccessor
    ) {
        input.reset(tupleAddress, sizeOfTupleAt(tupleAddress, memoryAccessor));
        try {
            for (int i = 0; i < 2; i++) {
                // Skip the size field
                input.readLong();
                final Object o = input.readOptimized();
                pair.set(i, o);
            }
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }

    public static long getLong(long base, long offset, MemoryAccessor memoryAccessor) {
        if (base == NULL_ADDRESS) {
            return NULL_ADDRESS;
        }
        return memoryAccessor.getLong(base + offset);
    }


    public static void putLong(long base, long offset, long value, MemoryAccessor memoryAccessor) {
        if (base == NULL_ADDRESS) {
            return;
        }
        memoryAccessor.putLong(base + offset, value);
    }

    public static byte getByte(long base, long offset, MemoryAccessor memoryAccessor) {
        if (base == NULL_ADDRESS) {
            return BYTE_0;
        }
        return memoryAccessor.getByte(base + offset);
    }

    public static void putByte(long base, long offset, byte value, MemoryAccessor memoryAccessor) {
        if (base == NULL_ADDRESS) {
            return;
        }
        memoryAccessor.putByte(base + offset, value);
    }

    private static long offsetOfValueSizeField(long keySize) {
        return KEY_BLOCK_OFFSET + keySize;
    }

}
