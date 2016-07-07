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
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.nio.Bits;

import java.io.IOException;

import static com.hazelcast.jet.memory.util.Util.B_ZERO;

/**
 * Utility methods related to I/O
 */
public final class JetIoUtil {
    public static final int KEY_BLOCK_SIZE_OFFSET = 0;
    public static final int KEY_BLOCK_OFFSET = 8;

    private JetIoUtil() {
    }

    public static long addressOfKeyBlockAt(long tupleAddress) {
        return tupleAddress + KEY_BLOCK_OFFSET;
    }

    public static long sizeOfKeyBlockAt(long tupleAddress, MemoryAccessor memoryAccessor) {
        return getLong(tupleAddress, KEY_BLOCK_SIZE_OFFSET, memoryAccessor);
    }

    public static long addrOfValueBlockAt(long tupleAddress, MemoryAccessor memoryAccessor) {
        long keySize = sizeOfKeyBlockAt(tupleAddress, memoryAccessor);
        return tupleAddress + (2 * Bits.LONG_SIZE_IN_BYTES) + keySize;
    }

    public static long sizeOfValueBlockAt(long tupleAddress, MemoryAccessor memoryAccessor) {
        long keySize = sizeOfKeyBlockAt(tupleAddress, memoryAccessor);
        return getLong(tupleAddress, Bits.LONG_SIZE_IN_BYTES + keySize, memoryAccessor);
    }

    public static long sizeOfTupleAt(long tupleAddress, MemoryAccessor memoryAccessor) {
        final long keySize = getLong(tupleAddress, KEY_BLOCK_SIZE_OFFSET, memoryAccessor);
        final long valueSize = getLong(tupleAddress, Bits.LONG_SIZE_IN_BYTES + keySize, memoryAccessor);
        return keySize + valueSize + (2 * Bits.LONG_SIZE_IN_BYTES);
    }

    public static void writeTuple(Tuple source, JetDataOutput output, IOContext ioContext, MemoryManager memoryManager) {
        output.clear();
        output.setMemoryManager(memoryManager);
        writeElements(true, source, output, ioContext, memoryManager.getAccessor());
        writeElements(false, source, output, ioContext, memoryManager.getAccessor());
    }

    public static void readTuple(
            JetDataInput input, long tupleAddress, Tuple destination, IOContext ioContext, MemoryAccessor memoryAccessor
    ) {
        input.reset(tupleAddress, sizeOfTupleAt(tupleAddress, memoryAccessor));
        readElements(ioContext, input, destination, true);
        readElements(ioContext, input, destination, false);
    }

    @SuppressWarnings("unchecked")
    public static void writeObject(Object object, IOContext ioContext, JetDataOutput output) throws IOException {
        ioContext.getDataType(object).getObjectWriter().write(object, output, ioContext.getObjectWriterFactory());
    }


    public static long getLong(long base, long offset, MemoryAccessor memoryAccessor) {
        if (base == MemoryAllocator.NULL_ADDRESS) {
            return MemoryAllocator.NULL_ADDRESS;
        }
        return memoryAccessor.getLong(base + offset);
    }

    public static void putLong(long base, long offset, long value, MemoryAccessor memoryAccessor) {
        if (base == MemoryAllocator.NULL_ADDRESS) {
            return;
        }
        memoryAccessor.putLong(base + offset, value);
    }

    public static byte getByte(long base, long offset, MemoryAccessor memoryAccessor) {
        if (base == MemoryAllocator.NULL_ADDRESS) {
            return B_ZERO;
        }
        return memoryAccessor.getByte(base + offset);
    }

    public static void putByte(long base, long offset, byte value, MemoryAccessor memoryAccessor) {
        if (base == MemoryAllocator.NULL_ADDRESS) {
            return;
        }
        memoryAccessor.putByte(base + offset, value);
    }

    private static  void writeElements(
            boolean doKeys, Tuple tuple, JetDataOutput output, IOContext ioContext, MemoryAccessor memoryAccessor
    ) {
        // Remember position of the size field
        final long initialPos = output.position();
        // Reserve space for the size field before its value is known
        output.skip(Bits.LONG_SIZE_IN_BYTES);
        // Remember initial block size, to calculate the size of what this call wrote
        final long initialSize = output.usedSize();
        final int elementCount = doKeys ? tuple.keyCount() : tuple.valueCount();
        try {
            output.writeInt(elementCount);
            for (int i = 0; i < elementCount; i++) {
                writeObject(tuple.get(doKeys, i), ioContext, output);
            }
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
        memoryAccessor.putLong(output.baseAddress() + initialPos, output.usedSize() - initialSize);
    }

    private static void readElements(IOContext ioContext, JetDataInput input, Tuple tuple, boolean copyKeys) {
        // Skip the size field
        try {
            input.readLong();
            int elementCount = input.readInt();
            for (int i = 0; i < elementCount; i++) {
                byte typeID = input.readByte();
                final Object o = ioContext.getDataType(typeID)
                                          .getObjectReader()
                                          .read(input, ioContext.getObjectReaderFactory());
                tuple.set(copyKeys, i, o);
            }
        } catch (IOException e) {
            throw Util.rethrow(e);
        }
    }
}
