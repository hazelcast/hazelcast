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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.ringbuffer.StaleSequenceException;

/**
 * The Ringbuffer is responsible for storing the actual content of a ringbuffer.
 *
 * @param <T> the type of the data stored in the ring buffer
 */
public interface Ringbuffer<T> {
    /**
     * Returns the capacity of this Ringbuffer.
     *
     * @return the capacity.
     */
    long getCapacity();

    /**
     * Returns number of items in the ringbuffer (meaning the number of items between the {@link #headSequence()} and
     * {@link #tailSequence()}).
     *
     * @return the size.
     */
    long size();

    /**
     * Returns the sequence of the tail. The tail is the side of the ringbuffer where the items are added to.
     * <p>
     * The initial value of the tail is -1.
     *
     * @return the sequence of the tail.
     */
    long tailSequence();

    /**
     * Sets the tail sequence. The tail sequence cannot be less than {@link #headSequence()} - 1.
     *
     * @param tailSequence the new tail sequence
     * @throws IllegalArgumentException if the target sequence is less than {@link #headSequence()} - 1
     */
    void setTailSequence(long tailSequence);

    /**
     * Returns the sequence of the head. The head is the side of the ringbuffer where the oldest items in the
     * ringbuffer are found.
     * <p>
     * If the RingBuffer is empty, the head will be one more than the {@link #tailSequence()}.
     * <p>
     * The initial value of the head is 0 (1 more than tail).
     *
     * @return the sequence of the head.
     */
    long headSequence();

    /**
     * Sets the head sequence. The head sequence cannot be larger than {@link #tailSequence()} + 1
     *
     * @param sequence The new head sequence.
     * @throws IllegalArgumentException if the target sequence is greater than {@link #tailSequence()} + 1
     */
    void setHeadSequence(long sequence);

    /**
     * Is the ring buffer empty.
     *
     * @return is the ring buffer empty
     */
    boolean isEmpty();

    /**
     * Adds an item to the tail of the Ringbuffer. If there is no space in the Ringbuffer, the add will overwrite the oldest
     * item in the ringbuffer. The method allows null values.
     * <p>
     * The returned value is the sequence of the added item. Using this sequence you can read the added item.
     * <p>
     * <h3>Using the sequence as id</h3>
     * This sequence will always be unique for this Ringbuffer instance so it can be used as a unique id generator if you are
     * publishing items on this Ringbuffer. However you need to take care of correctly determining an initial id when any node
     * uses the ringbuffer for the first time. The most reliable way to do that is to write a dummy item into the ringbuffer and
     * use the returned sequence as initial id. On the reading side, this dummy item should be discard. Please keep in mind that
     * this id is not the sequence of the item you are about to publish but from a previously published item. So it can't be used
     * to find that item.
     *
     * @param item the item to add.
     * @return the sequence of the added item.
     */
    long add(T item);

    /**
     * Reads one item from the Ringbuffer.
     * <p>
     * This method is not destructive unlike e.g. a queue.take. So the same item can be read by multiple readers or it can be
     * read multiple times by the same reader.
     *
     * @param sequence the sequence of the item to read.
     * @return the read item
     * @throws StaleSequenceException if the sequence is smaller then {@link #headSequence()}
     *                                or larger than {@link #tailSequence()}. Because a Ringbuffer won't store all event
     *                                indefinitely, it can be that the data for the given sequence doesn't exist anymore
     *                                and the {@link StaleSequenceException} is thrown. It is up to the caller to deal with
     *                                this particular situation, e.g. throw an Exception or restart from the last known head.
     *                                That is why the StaleSequenceException contains the last known head.
     */
    T read(long sequence);

    /**
     * Check if the sequence can be read from the ring buffer or if the sequence is of the next item to be added into the
     * ringbuffer. This method also allows the sequence to be one greater than the {@link #tailSequence()}, giving the
     * oportunity to block until the item is added into the ring buffer.
     *
     * @param readSequence the sequence wanting to be read
     * @throws StaleSequenceException if the requested sequence is greater than the {@link #tailSequence()} + 1 or smaller
     *                                than the {@link #headSequence()}
     */
    void checkBlockableReadSequence(long readSequence);

    /**
     * Check if the sequence can be read from the ring buffer.
     *
     * @param sequence the sequence wanting to be read
     * @throws StaleSequenceException if the requested sequence is greater than the {@link #tailSequence()} or smaller than the
     *                                {@link #headSequence()}
     */
    void checkReadSequence(long sequence);

    /**
     * Sets the item at the given sequence. The method allows null data.
     *
     * @param seq  The target sequence.
     * @param data The data to be set
     */
    void set(long seq, T data);
}
