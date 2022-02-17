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

import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.util.hashslot.HashSlotArray8byteKey;
import com.hazelcast.internal.util.hashslot.HashSlotCursor8byteKey;
import com.hazelcast.internal.util.hashslot.impl.HashSlotArray8byteKeyNoValue;

import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_CAPACITY;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.DEFAULT_LOAD_FACTOR;

/**
 * {@link LongSet} implementation based on {@link HashSlotArray8byteKey}.
 */
public class LongSetHsa implements LongSet {

    private final long nullValue;
    private final HashSlotArray8byteKey hsa;

    public LongSetHsa(long nullValue, MemoryManager memMgr) {
        this(nullValue, memMgr, DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public LongSetHsa(long nullValue, MemoryManager memMgr, int initialCapacity, float loadFactor) {
        this.nullValue = nullValue;
        this.hsa = new HashSlotArray8byteKeyNoValue(nullValue, memMgr, initialCapacity, loadFactor);
        hsa.gotoNew();
    }

    @Override
    public boolean add(long value) {
        assert value != nullValue : "add() called with null-sentinel value " + nullValue;
        return hsa.ensure(value).isNew();
    }

    @Override
    public boolean remove(long value) {
        assert value != nullValue : "remove() called with null-sentinel value " + nullValue;
        return hsa.remove(value);
    }

    @Override
    public boolean contains(long value) {
        assert value != nullValue : "contains() called with null-sentinel value " + nullValue;
        return hsa.get(value) != NULL_ADDRESS;
    }

    @Override
    public long size() {
        return hsa.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void clear() {
        hsa.clear();
    }

    @Override
    public LongCursor cursor() {
        assert hsa.address() >= 0 : "cursor() called on a disposed map";
        return new Cursor();
    }

    @Override
    public void dispose() {
        hsa.dispose();
    }

    private final class Cursor implements LongCursor {

        private final HashSlotCursor8byteKey hsaCursor = hsa.cursor();

        @Override
        public boolean advance() {
            return hsaCursor.advance();
        }

        @Override
        public long value() {
            return hsaCursor.key();
        }

        @Override
        public void reset() {
            hsaCursor.reset();
        }
    }
}
