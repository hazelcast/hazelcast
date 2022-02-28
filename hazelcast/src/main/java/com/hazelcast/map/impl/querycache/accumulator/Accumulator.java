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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Accumulator for events which have a sequence number.
 * Only allows read access to elements in it, except {@link Accumulator#reset()} method.
 *
 * @param <E> the allowed type for this accumulator to accumulate.
 * @see Sequenced
 */
public interface Accumulator<E extends Sequenced> extends Iterable<E> {

    /**
     * Adds event to this accumulator.
     *
     * @param event event to add.
     */
    void accumulate(E event);

    /**
     * Reads this accumulator if it contains at least {@code maxItems}, otherwise
     * do not read anything. If this method adds items to the supplied handler, head of this accumulator advances.
     *
     * Used for batching purposes.
     *
     * @param handler  handler to process this accumulator.
     * @param maxItems read this accumulator if it contains at least {@code maxItems}
     * @return the number of elements read.
     */
    int poll(AccumulatorHandler<E> handler, int maxItems);

    /**
     * Reads all expired items to the supplied handler.
     * If this method adds items to the supplied handler, head of this accumulator advances.
     *
     * @param handler handler to process this accumulator.
     * @param delay   after this duration element will be poll-able.
     * @param unit    time-unit
     * @return the number of elements polled.
     */
    int poll(AccumulatorHandler<E> handler, long delay, TimeUnit unit);

    /**
     * Returns an iterator over the items in this accumulator in proper sequence.
     * Also this methods advances head of this accumulator.
     *
     * @return an iterator over the items in this list accumulator in proper sequence.
     */
    @Override
    Iterator<E> iterator();

    /**
     * Returns {@link AccumulatorInfo}  of this accumulator.
     *
     * @return {@link AccumulatorInfo}  of this accumulator.
     */
    AccumulatorInfo getInfo();

    /**
     * Tries to set head of this accumulator to the supplied {@code sequence} and returns {@code true},
     * if that {@code sequence} is still exist in this accumulator. Otherwise, returns {@code false}.
     *
     * @param sequence the sequence number which will be the head of this accumulator.
     * @return {@code true} if {@code sequence} is set, otherwise returns {@code false}
     */
    boolean setHead(long sequence);

    /**
     * Current size of accumulator.
     *
     * @return returns current size of accumulator.
     */
    int size();

    /**
     * Returns <tt>true</tt> if this accumulator contains no elements.
     *
     * @return <tt>true</tt> if this accumulator contains no elements
     */
    boolean isEmpty();

    /**
     * Resets this accumulator.
     *
     * After return of this call, accumulator will be in its initial state.
     */
    void reset();
}
