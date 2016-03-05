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

package com.hazelcast.jet.memory.api.binarystorage.sorted;

import com.hazelcast.jet.memory.spi.binarystorage.BinaryComparator;
import com.hazelcast.jet.memory.api.binarystorage.BinaryKeyValueStorage;
import com.hazelcast.jet.memory.spi.binarystorage.sorted.OrderingDirection;
import com.hazelcast.jet.memory.api.binarystorage.iterator.BinarySlotSortedIterator;

/**
 * Represents API for sorted binary key-value storage
 */
public interface BinaryKeyValueSortedStorage<T> extends BinaryKeyValueStorage<T> {
    /**
     * @param direction -
     *                  ASC - 1;
     *                  DESC - 0;
     * @return address of the first slot address in accordance with direction;
     */
    long first(OrderingDirection direction);

    /**
     * @param slotAddress - slot address to start from;
     * @param direction   -
     *                    ASC - 1;
     *                    DESC - 0;
     * @return next slot address in accordance with direction;
     */
    long getNext(long slotAddress,
                 OrderingDirection direction);

    /**
     * Sort storage with corresponding direction;
     *
     * @param direction ASC - 1;
     *                  DESC - 0;
     * @return iterator with iterating direction corresponding to direction;
     */
    BinarySlotSortedIterator slotIterator(OrderingDirection direction);

    /**
     * Set default comparator to be used;
     *
     * @param comparator - comparator to be used;
     */
    void setComparator(BinaryComparator comparator);

    /**
     * Initialize current object as flight-weight object under sorted storage;
     */
    void setSorted(OrderingDirection direction);
}
