/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import java.util.Collection;
import java.util.List;

/**
 * A specific queue implementation which is used for write-behind-store operations.
 * Also supports some filtering methods e.g. {@link #getFrontByTime}, {@link  #getFrontByNumber}
 *
 * @param <E> the type of element to be stored in this queue.
 */
public interface WriteBehindQueue<E> {

    /**
     * Inserts collection of elements to the front of this queue.
     *
     * @param collection collection of elements to be inserted in front of this queue.
     */
    void addFirst(Collection<E> collection);

    /**
     * Inserts to the end of this queue.
     *
     * @param e element to be offered
     */
    void addLast(E e);

    /**
     * Removes the first occurrence of the specified element in this queue
     * when searching it by starting from the head of this queue.
     *
     * @param e element to be removed.
     * @return <code>true</code> if removed successfully, <code>false</code> otherwise
     */
    boolean removeFirstOccurrence(E e);

    /**
     * Removes all elements from this queue and adds them to the given collection.
     *
     * @param collection all elements to be added to this collection.
     * @return number of removed items from this queue.
     */
    int drainTo(Collection<E> collection);

    /**
     * Checks whether an element exist in this queue.
     *
     * @param e item to be checked
     * @return <code>true</code> if exists, <code>false</code> otherwise
     */
    boolean contains(E e);

    /**
     * Returns the number of elements in this {@link WriteBehindQueue}.
     *
     * @return the number of elements in this {@link WriteBehindQueue}.
     */
    int size();

    /**
     * Removes all of the elements in this  {@link WriteBehindQueue}
     * Queue will be empty after this method returns.
     */
    void clear();

    /**
     * Returns a read-only list representation of this queue.
     *
     * @return read-only list representation of this queue.
     */
    List<E> asList();

    /**
     * Adds all elements to the supplied collection which are smaller than or equal to the given time.
     * Finds elements which's delayed times were elapsed. Used in usual flow of write-behind execution.
     *
     * @param time       given time.
     * @param collection all found elements will be added to this collection.
     */
    void getFrontByTime(long time, Collection<E> collection);

    /**
     * Adds the given number of elements to the supplied collection by starting from the head of this queue.
     * If there is a need to immediately flush some number of elements from this queue, this method will be used.
     *
     * @param numberOfElements get this number of elements from the start of this queue.
     * @param collection       all found elements will be added to this collection.
     */
    void getFrontByNumber(int numberOfElements, Collection<E> collection);

}

