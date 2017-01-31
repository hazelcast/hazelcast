/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

/**
 * Ringbuffer store makes a ring buffer backed by a central data store; such as database, disk, etc.
 *
 * @param <T> ring buffer item type
 */
public interface RingbufferStore<T> {

    /**
     * Stores one item with it's corresponding sequence.
     *
     * @param sequence the sequence ID of the data
     * @param data     the value of the data to store
     */
    void store(long sequence, T data);

    /**
     * Stores multiple entries. Implementation of this method can optimize the store operation.
     *
     * @param firstItemSequence the sequence of the first item
     * @param items             the items that are being stored
     */
    void storeAll(long firstItemSequence, T[] items);

    /**
     * Loads the value of a given sequence. Null value means that the item was not found.
     *
     * @param sequence the sequence of the requested item
     * @return requested item, null if not found
     */
    T load(long sequence);

    /**
     * Return the largest sequence seen by the data store. Can return -1 if the data store doesn't have or can't access any data.
     *
     * @return the largest sequence of the data in the data store
     */
    long getLargestSequence();
}
