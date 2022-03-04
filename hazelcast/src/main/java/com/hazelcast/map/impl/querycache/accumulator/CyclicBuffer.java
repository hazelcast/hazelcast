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

package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;

/**
 * A cyclic buffer to store {@link Sequenced} events. Only additions can be done to this buffer.
 * If capacity is not sufficient subsequent additions will be done by overwriting existing ones.
 * Also read access to any element is possible by providing the sequence number.
 *
 * @param <E> the type of elements in this buffer.
 */
public interface CyclicBuffer<E extends Sequenced> {

    /**
     * Adds an event to this buffer and sets tail to the sequence of given event.
     *
     * @param event the event to be added.
     */
    void add(E event);

    /**
     * Returns next unread event from this buffer and advances head sequence of this buffer by one.
     *
     * @return next unread event.
     */
    E getAndAdvance();

    /**
     * Returns the element in this buffer which has the supplied {@code sequence}.
     *
     * @param sequence the sequence number of the event.
     * @return the element with the supplied {@code sequence}.
     */
    E get(long sequence);

    /**
     * Sets head of this buffer to the supplied {@code sequence} and returns {@code true}, if that {@code sequence}
     * is still in this buffer. Otherwise, returns {@code false}
     *
     * @param sequence the sequence number which will be set to head of this buffer.
     * @return {@code true} if {@code sequence} is set to head, otherwise returns {@code false}.
     */
    boolean setHead(long sequence);

    /**
     * Returns the sequence number of the element in the head index of this buffer if there exists any,
     * otherwise {@code -1L} will be returned.
     *
     * @return the sequence number of the element in the head index or {@code -1L}.
     */
    long getHeadSequence();

    /**
     * Resets this buffer; after the return of this method buffer will be empty and
     * all elements in it will be null.
     */
    void reset();

    /**
     * Returns the size of unread events in this buffer according to the current head.
     *
     * @return the size of unread events in this buffer.
     */
    int size();
}
