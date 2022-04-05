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

package com.hazelcast.ringbuffer;

import com.hazelcast.core.IFunction;

/**
 * Ringbuffer store makes a ring buffer backed by a central data store; such as database, disk, etc.
 *
 * @param <T> ring buffer item type
 */
public interface RingbufferStore<T> {

    /**
     * Stores one item with it's corresponding sequence.
     *
     * Exceptions thrown by this method prevent {@link com.hazelcast.ringbuffer.Ringbuffer}
     * from adding the item. It is the responsibility of the store to
     * ensure that it keeps consistent with the Ringbuffer and in case
     * of an exception is thrown, the data is not in the store.
     *
     * @param sequence the sequence ID of the data
     * @param data     the value of the data to store
     * @see com.hazelcast.ringbuffer.Ringbuffer#add(Object)
     * @see com.hazelcast.ringbuffer.Ringbuffer#addAsync(Object, com.hazelcast.ringbuffer.OverflowPolicy)
     */
    void store(long sequence, T data);

    /**
     * Stores multiple items. Implementation of this method can optimize the store operation.
     *
     * Exceptions thrown by this method prevents {@link com.hazelcast.ringbuffer.Ringbuffer}
     * from adding any of the items. It is the responsibility of the store
     * to ensure that it keeps consistent with the Ringbuffer and in case
     * of an exception is thrown the passed data is not in the store.
     *
     * @param firstItemSequence the sequence of the first item
     * @param items             the items that are being stored
     * @see com.hazelcast.ringbuffer.Ringbuffer#addAllAsync(java.util.Collection, com.hazelcast.ringbuffer.OverflowPolicy)
     */
    void storeAll(long firstItemSequence, T[] items);

    /**
     * Loads the value of a given sequence. Null value means that the item
     * was not found. This method is invoked by the ringbuffer if an item
     * is requested with a sequence which is no longer in the Ringbuffer.
     *
     * @param sequence the sequence of the requested item
     * @return requested item, null if not found
     * @see com.hazelcast.ringbuffer.Ringbuffer#readOne(long)
     * @see com.hazelcast.ringbuffer.Ringbuffer#readManyAsync(long, int, int, IFunction)
     */
    T load(long sequence);

    /**
     * Return the largest sequence seen by the data store. Can return -1 if
     * the data store doesn't have or can't access any data. Used to
     * initialize a Ringbuffer with the previously seen largest sequence number.
     *
     * @return the largest sequence of the data in the data store
     */
    long getLargestSequence();
}
