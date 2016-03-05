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

package com.hazelcast.jet.memory.impl.util;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.jet.memory.api.binarystorage.oalayout.OALayout;
import com.hazelcast.jet.memory.impl.binarystorage.JetHashSlotArray8ByteKeyImpl;
import com.hazelcast.jet.memory.spi.memory.MemoryPool;

public final class MemoryUtil {
    public static final long NULL_VALUE = MemoryAllocator.NULL_ADDRESS;

    private MemoryUtil() {
    }

    public static void checkNonNegativeInt(long v) {
        assert v >= 0 && v <= Integer.MAX_VALUE;
    }

    public static String dumpHeap(MemoryPool memoryPool) {
        return " {Total=" + memoryPool.getTotal() + " Used=" + memoryPool.getUsed() + "}";
    }

    public static void validateAddress(long address, long limit) {
        assert ((address >= 0) && (address < limit))
                : "invalid address " + address + " limit " + limit;
    }

    public static long toSlotAddress(long valueBlockAddress) {
        if (valueBlockAddress == MemoryUtil.NULL_VALUE) {
            return MemoryUtil.NULL_VALUE;
        } else {
            return JetHashSlotArray8ByteKeyImpl.valueAddr2slotBase(
                    Math.abs(valueBlockAddress)
            );
        }
    }

    public static long nvl(long address, OALayout layout) {
        if (address < layout.getHashSlotAllocator().address()) {
            return MemoryUtil.NULL_VALUE;
        }

        if (address > layout.getHashSlotAllocator().address()
                +
                (layout.getHashSlotAllocator().size() - 1) * layout.getSlotSizeInBytes()) {
            return MemoryUtil.NULL_VALUE;
        }

        return address;
    }

}
