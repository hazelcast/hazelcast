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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.ringbuffer.StaleSequenceException;

/**
 * The Ringbuffer is responsible for storing the actual content of a
 * ringbuffer.
 *
 * @param <E> the type of the data stored in the ringbuffer
 */
public interface Ringbuffer<E> extends Iterable<E> {

    /**
     * Returns the capacity of this ringbuffer.
     *
     * @return the capacity
     */
    long getCapacity();

    /**
     * Returns number of items in the ringbuffer (meaning the number of items
     * between the {@link #headSequence()} and {@link #tailSequence()}).
     *
     * @return the size
     */
    long size();

    /**
     * Returns the sequence of the tail. The tail is the side of the ringbuffer
     * where the items are added to.
     * The initial value of the tail is {@code -1}.
     *
     * @return the sequence of the tail
     */
    long tailSequence();

    /**
     * Returns the next sequence which the tail will be at after the next item
     * is added. Please note that there is no item in the Ringbuffer with the
     * returned sequence at the time of the call.
     *
     * @return the next sequence of the tail
     */
    long peekNextTailSequence();

    /**
     * Sets the tail sequence. The tail sequence cannot be less than
     * {@code headSequence() - 1}.
     *
     * @param tailSequence the new tail sequence
     * @throws IllegalArgumentException if the target sequence is less than
     *                                  {@code headSequence() - 1}
     * @see #headSequence()
     */
    void setTailSequence(long tailSequence);

    /**
     * Returns the sequence of the head. The head is the side of the ringbuffer
     * where the oldest items in the ringbuffer are found.
     * If the RingBuffer is empty, the head will be one more than the
     * {@link #tailSequence()}.
     * The initial value of the head is 0 (1 more than tail).
     *
     * @return the sequence of the head
     */
    long headSequence();

    /**
     * Sets the head sequence. The head sequence cannot be larger than
     * {@code tailSequence() + 1}
     *
     * @param sequence the new head sequence
     * @throws IllegalArgumentException if the target sequence is greater than {@code tailSequence() + 1}
     * @see #tailSequence()
     */
    void setHeadSequence(long sequence);

    /**
     * Checks if the ringbuffer is empty.
     *
     * @return {@code true} if the ringbuffer is empty, {@code false} otherwise
     */
    boolean isEmpty();

    /**
     * Adds an item to the tail of the ringbuffer. If there is no space in the
     * ringbuffer, the add will overwrite the oldest item in the ringbuffer.
     * The method allows null values.
     * <p>
     * The returned value is the sequence of the added item. Using this sequence
     * you can read the added item.
     * <h3>Using the sequence as ID</h3>
     * This sequence will always be unique for this ringbuffer instance so it
     * can be used as a unique ID generator if you are publishing items on this
     * ringbuffer. However you need to take care of correctly determining an
     * initial ID when any node uses the ringbuffer for the first time. The most
     * reliable way to do that is to write a dummy item into the ringbuffer and
     * use the returned sequence as initial ID. On the reading side, this dummy
     * item should be discard. Please keep in mind that this ID is not the
     * sequence of the item you are about to publish but from a previously
     * published item. So it can't be used to find that item.
     *
     * @param item the item to add
     * @return the sequence of the added item
     */
    long add(E item);

    /**
     * Reads one item from the ringbuffer.
     * This method is not destructive unlike e.g. a {@code queue.take}. So the
     * same item can be read by multiple readers or it can be read multiple
     * times by the same reader.
     *
     * @param sequence the sequence of the item to read
     * @return the read item
     * @throws StaleSequenceException if the sequence is smaller then {@link #headSequence()} or larger than
     *                                {@link #tailSequence()}. Because a ringbuffer won't store all event
     *                                indefinitely, it can be that the data for the given sequence doesn't
     *                                exist anymore and the {@link StaleSequenceException} is thrown.
     *                                It is up to the caller to deal with this particular situation, e.g.
     *                                throw an {@code Exception} or restart from the last known head.
     *                                That is why the StaleSequenceException contains the last known head.
     */
    E read(long sequence);

    /**
     * Check if the sequence can be read from the ringbuffer or if the sequence
     * is of the next item to be added into the ringbuffer. This method also
     * allows the sequence to be one greater than the {@link #tailSequence()},
     * giving the opportunity to block until the item is added into the ringbuffer.
     *
     * @param readSequence the sequence wanting to be read
     * @throws StaleSequenceException   if the requested sequence is smaller than the {@link #headSequence()}
     * @throws IllegalArgumentException if the requested sequence is greater than the {@code tailSequence() + 1}
     * @see #headSequence()
     * @see #tailSequence()
     */
    void checkBlockableReadSequence(long readSequence);

    /**
     * Check if the sequence can be read from the ringbuffer.
     *
     * @param sequence the sequence wanting to be read
     * @throws StaleSequenceException   if the requested sequence is smaller than the {@link #headSequence()}
     * @throws IllegalArgumentException if the requested sequence is greater than the {@link #tailSequence()}
     * @see #headSequence()
     * @see #tailSequence()
     */
    void checkReadSequence(long sequence);

    /**
     * Sets the item at the given sequence. The method allows {@code null} data.
     *
     * @param seq  the target sequence
     * @param data the data to be set
     */
    void set(long seq, E data);

    /**
     * Clears the data in the ringbuffer.
     */
    void clear();

    /**
     * Returns the array representing this ringbuffer.
     * Items at the beginning of this array may be newer than items at the end
     * of this array. This means that this array is not sorted by sequence ID
     * and the index of the item in this array must be calculated using the
     * sequence and the modulo of the array.
     */
    E[] getItems();
}
