/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.writebehind;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main contract for write behind queues
 * which are used for map store operations.
 *
 * @param <E> Type of entry to be stored.
 */
public interface WriteBehindQueue<E> {

    public static final AtomicInteger OFFER = new AtomicInteger(0);
    public static final AtomicInteger REMOVE = new AtomicInteger(0);
    /**
     * adds to the end.
     */
    boolean offer(E e);

    /**
     * removes head of the queue.
     */
    void removeFirst();

    /**
     * Reads item at that index in queue.
     *
     * @param index index of item.
     * @return item at index or <tt>null</tt>
     * if index is out of bounds.
     */
    E get(int index);

    /**
     * @return {@code true} if at least one
     * element present like {@code o.equals(e)}.
     */
    boolean contains(E o);

    int size();

    void clear();

    /**
     * @return A copy of queue at that moment.
     * Returned copy has same characteristics with the original.
     */
    WriteBehindQueue<E> getSnapShot();

    /**
     * Add this collection to the front of the queue.
     *
     * @param collection
     */
    void addFront(Collection<E> collection);

    /**
     * Add this collection to the end of the queue.
     *
     * @param collection
     */
    void addEnd(Collection<E> collection);


    /**
     * Returns all in this queue and clears the queue.
     */
    List<E>  fetchAndRemoveAll();

    /**
     * TODO is enabled really needed?
     * <p/>
     * Empty or a real queue.
     */
    boolean isEnabled();

    /**
     * @return list representation of this queue.
     */
    List<E> asList();

    /**
     * Shrinks the size. This can be used to minimize
     * the storage of an <tt>WriteBehindQueue</tt> instance.
     */
    void shrink();
}

