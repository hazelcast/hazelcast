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

package com.hazelcast.map.mapstore.writebehind;

import java.util.Collection;
import java.util.List;

/**
 * Main contract for write behind queues
 * which are used for map store operations.
 *
 * @param <E> Type of entry to be stored.
 */
public interface WriteBehindQueue<E> extends Iterable<E> {

    /**
     * adds to the end.
     *
     * @param e item to be offered
     * @return <code>true</code> if added, <code>false</code> otherwise.
     */
    boolean offer(E e);

    /**
     * Gets item.
     *
     * @param e item to be offered
     * @return corresponding item from queue or null.
     */
    E get(E e);

    /**
     * removes head of the queue.
     */
    void removeFirst();

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
     * Removes all items in collection from queue.
     *
     * @param collection collection to be removed.
     */
    void removeAll(Collection<E> collection);


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
     * Returns list of entries smaller than specific time.
     *
     * @param now now in millis.
     * @return entries to process according to time.
     */
    List<E> filterItems(long now);

}

