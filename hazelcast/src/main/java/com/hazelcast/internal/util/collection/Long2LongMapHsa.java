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

package com.hazelcast.internal.util.collection;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.internal.util.hashslot.SlotAssignmentResult;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray8byteKeyImpl;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;

/**
 * a {@link Long2LongMap} implemented in terms of a {@link HashSlotArray8byteKey}.
 */
public class Long2LongMapHsa implements Long2LongMap {

    private final HashSlotArray8byteKey hsa;
    private final long nullValue;
    private MemoryAccessor mem;

    /**
     * @param nullValue the value that represents "null" or missing value
     * @param memMgr memory manager to use. It is safe for its {@link MemoryManager#getAccessor} method
     *               to return an accessor that only supports aligned memory access.
     */
    public Long2LongMapHsa(long nullValue, MemoryManager memMgr) {
        this.hsa = new HashSlotArray8byteKeyImpl(nullValue, memMgr, LONG_SIZE_IN_BYTES);
        hsa.gotoNew();
        this.mem = memMgr.getAccessor();
        this.nullValue = nullValue;
    }

    @Override public long get(long key) {
        final long valueAddr = hsa.get(key);
        return valueAddr != NULL_ADDRESS ? mem.getLong(valueAddr) : nullValue;
    }

    @Override public long put(long key, long value) {
        assert value != nullValue : "put() called with null-sentinel value " + nullValue;
        SlotAssignmentResult slot = hsa.ensure(key);
        long result;
        if (!slot.isNew()) {
            result = mem.getLong(slot.address());
        } else {
            result = nullValue;
        }
        mem.putLong(slot.address(), value);
        return result;
    }

    @Override public long putIfAbsent(long key, long value) {
        assert value != nullValue : "putIfAbsent() called with null-sentinel value " + nullValue;
        SlotAssignmentResult slot = hsa.ensure(key);
        if (slot.isNew()) {
            mem.putLong(slot.address(), value);
            return nullValue;
        } else {
            return mem.getLong(slot.address());
        }
    }

    @Override public void putAll(Long2LongMap from) {
        for (LongLongCursor cursor = from.cursor(); cursor.advance();) {
            put(cursor.key(), cursor.value());
        }
    }

    @Override public boolean replace(long key, long oldValue, long newValue) {
        assert oldValue != nullValue : "replace() called with null-sentinel oldValue " + nullValue;
        assert newValue != nullValue : "replace() called with null-sentinel newValue " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return false;
        }
        final long actualValue = mem.getLong(valueAddr);
        if (actualValue != oldValue) {
            return false;
        }
        mem.putLong(valueAddr, newValue);
        return true;
    }

    @Override public long replace(long key, long value) {
        assert value != nullValue : "replace() called with null-sentinel value " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return nullValue;
        }
        final long oldValue = mem.getLong(valueAddr);
        mem.putLong(valueAddr, value);
        return oldValue;
    }

    @Override public long remove(long key) {
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return nullValue;
        }
        final long oldValue = mem.getLong(valueAddr);
        hsa.remove(key);
        return oldValue;
    }

    @Override public boolean remove(long key, long value) {
        assert value != nullValue : "remove() called with null-sentinel value " + nullValue;
        final long valueAddr = hsa.get(key);
        if (valueAddr == NULL_ADDRESS) {
            return false;
        }
        final long actualValue = mem.getLong(valueAddr);
        if (actualValue == value) {
            hsa.remove(key);
            return true;
        }
        return false;
    }

    @Override public boolean containsKey(long key) {
        return hsa.get(key) != NULL_ADDRESS;
    }

    @Override public long size() {
        return hsa.size();
    }

    @Override public boolean isEmpty() {
        return hsa.size() == 0;
    }

    @Override public void clear() {
        hsa.clear();
    }

    @Override public void dispose() {
        hsa.dispose();
    }

    @Override public LongLongCursor cursor() {
        return new Cursor(hsa);
    }

    private final class Cursor implements LongLongCursor {

        private final HashSlotCursor8byteKey cursor;

        Cursor(HashSlotArray8byteKey hsa) {
            this.cursor = hsa.cursor();
        }

        @Override public boolean advance() {
            return cursor.advance();
        }

        @Override public long key() {
            return cursor.key();
        }

        @Override public long value() {
            return mem.getLong(cursor.valueAddress());
        }
    }
}
