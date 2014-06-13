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

/**
 * Main contract for write behind queues
 * which are used for map store operations.
 *
 * @param <E> Type of entry to be stored.
 */
public interface WriteBehindQueue<E> {

    /**
     * adds to the end.
     *
     * @param e item to be offered
     * @return <code>true</code> if added, <code>false</code> otherwise.
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
     * Removes item in that index from queue.
     *
     * @param index index of item.
     * @return item removed or <tt>null</tt>
     * if index is out of bounds.
     */
    E remove(int index);


    int size();

    void clear();

    /**
     * @return A copy of queue at that moment. Returned copy has same characteristics with the original.
     */
    WriteBehindQueue<E> getSnapShot();

    /**
     * Add this collection to the front of the queue.
     *
     * @param collection collection to be added in front of this queue.
     */
    void addFront(Collection<E> collection);

    /**
     * Add this collection to the end of the queue.
     *
     * @param collection collection to be added end of this queue.
     */
    void addEnd(Collection<E> collection);


    /**
     * Removes and returns all items in this queue.
     *
     * @return removed items in this queue.
     */
    List<E> removeAll();

    /**
     * Empty or a real queue.
     */
    boolean isEnabled();

    /**
     * Returns list representation of this queue.
     *
     * @return list representation of this queue.
     */
    List<E> asList();

    /**
     * Shrinks the size. This can be used to minimize
     * the storage of an <tt>WriteBehindQueue</tt> instance.
     */
    void shrink();
}

