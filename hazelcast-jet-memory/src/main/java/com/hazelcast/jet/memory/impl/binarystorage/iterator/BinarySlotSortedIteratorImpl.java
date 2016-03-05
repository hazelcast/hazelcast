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

package com.hazelcast.jet.memory.impl.binarystorage.iterator;

import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotSortedIterator;
import com.hazelcast.jet.memory.api.binarystorage.sorted.BinaryKeyValueSortedStorage;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.impl.util.MemoryUtil;

import java.util.NoSuchElementException;

/**
 * Record iterator over sorted binary storage.
 */
public class BinarySlotSortedIteratorImpl
        implements BinarySlotSortedIterator {
    private long pointer;
    private OrderingDirection direction;
    private final BinaryKeyValueSortedStorage storage;

    public BinarySlotSortedIteratorImpl(BinaryKeyValueSortedStorage storage) {
        this.storage = storage;
    }

    @Override
    public void setDirection(OrderingDirection direction) {
        this.direction = direction;
        reset();
    }

    @Override
    public boolean hasNext() {
        return pointer != MemoryUtil.NULL_VALUE;
    }

    @Override
    public long next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        long result = pointer;
        pointer = storage.getNext(pointer, direction);
        return result;
    }

    @Override
    public void reset() {
        if (direction != null) {
            pointer = storage.first(direction);
        } else {
            pointer = MemoryUtil.NULL_VALUE;
        }
    }
}
