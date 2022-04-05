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

package com.hazelcast.spi.merge;

import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.Ringbuffer;
import com.hazelcast.spi.impl.merge.RingbufferMergingValueImpl;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Iterator;

/**
 * A ringbuffer implementation holding data involved in split-brain healing.
 *
 * @see RingbufferMergingValueImpl
 * @since 3.10
 */
public class RingbufferMergeData implements Iterable<Object> {

    private Object[] items;
    private long tailSequence = -1;
    private long headSequence = tailSequence + 1;

    @SuppressWarnings("unchecked")
    public RingbufferMergeData(int capacity) {
        this.items = new Object[capacity];
    }

    public RingbufferMergeData(Ringbuffer<Object> ringbuffer) {
        this.items = ringbuffer.getItems();
        this.headSequence = ringbuffer.headSequence();
        this.tailSequence = ringbuffer.tailSequence();
    }

    /**
     * Returns the sequence of the tail. The tail is the side of the ringbuffer
     * where the items are added to.
     * <p>
     * The initial value of the tail is {@code -1}.
     *
     * @return the sequence of the tail
     */
    public long getTailSequence() {
        return tailSequence;
    }

    /**
     * Sets the tail sequence. The tail sequence cannot be less than
     * {@code headSequence() - 1}.
     *
     * @param sequence the new tail sequence
     * @throws IllegalArgumentException if the target sequence is less than
     *                                  {@code headSequence() - 1}
     * @see #getHeadSequence()
     */
    public void setTailSequence(long sequence) {
        this.tailSequence = sequence;
    }

    /**
     * Returns the sequence of the head. The head is the side of the ringbuffer
     * where the oldest items in the ringbuffer are found.
     * <p>
     * If the RingBuffer is empty, the head will be one more than the
     * {@link #getTailSequence()}.
     * <p>
     * The initial value of the head is 0 (1 more than tail).
     *
     * @return the sequence of the head
     */
    public long getHeadSequence() {
        return headSequence;
    }

    /**
     * Sets the head sequence. The head sequence cannot be larger than
     * {@code tailSequence() + 1}.
     *
     * @param sequence the new head sequence
     * @throws IllegalArgumentException if the target sequence is greater than {@code tailSequence() + 1}
     * @see #getTailSequence()
     */
    public void setHeadSequence(long sequence) {
        this.headSequence = sequence;
    }

    /**
     * Returns the capacity of this ringbuffer.
     *
     * @return the capacity
     */
    public int getCapacity() {
        return items.length;
    }

    /**
     * Returns number of items in the ringbuffer (meaning the number of items
     * between the {@link #getHeadSequence()} and {@link #getTailSequence()}).
     *
     * @return the size
     */
    public int size() {
        return (int) (tailSequence - headSequence + 1);
    }

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
    public long add(Object item) {
        tailSequence++;

        if (tailSequence - items.length == headSequence) {
            headSequence++;
        }

        int index = toIndex(tailSequence);

        items[index] = item;

        return tailSequence;
    }

    /**
     * Reads one item from the ringbuffer.
     *
     * @param sequence the sequence of the item to read
     * @param <E>      ringbuffer item type
     * @return the ringbuffer item
     * @throws StaleSequenceException if the sequence is smaller then {@link #getHeadSequence()}
     *                                or larger than {@link #getTailSequence()}
     */
    @SuppressWarnings("unchecked")
    public <E> E read(long sequence) {
        checkReadSequence(sequence);
        return (E) items[toIndex(sequence)];
    }

    /**
     * Sets the item at the given sequence. The method allows {@code null} data.
     *
     * @param seq  the target sequence
     * @param data the data to be set
     */
    public void set(long seq, Object data) {
        items[toIndex(seq)] = data;
    }

    /**
     * Clears the data in the ringbuffer.
     */
    public void clear() {
        Arrays.fill(items, null);
        tailSequence = -1;
        headSequence = tailSequence + 1;
    }

    private void checkReadSequence(long sequence) {
        if (sequence > tailSequence) {
            throw new IllegalArgumentException("sequence:" + sequence
                    + " is too large. The current tailSequence is:" + tailSequence);
        }

        if (sequence < headSequence) {
            throw new StaleSequenceException("sequence:" + sequence
                    + " is too small. The current headSequence is:" + headSequence
                    + " tailSequence is:" + tailSequence, headSequence);
        }
    }

    private int toIndex(long sequence) {
        return (int) (sequence % items.length);
    }

    /**
     * Returns the array representing this ringbuffer.
     * <p>
     * Items at the beginning of this array may have higher sequence IDs than
     * items at the end of this array. This means that this array is not sorted
     * by sequence ID and the index of the item in this array must be calculated
     * using the sequence and the modulo of the array.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Object[] getItems() {
        return items;
    }

    /**
     * {@inheritDoc}
     * Returns a read-only iterator. Mutation methods will throw a
     * {@link UnsupportedOperationException}.
     */
    @Override
    public Iterator<Object> iterator() {
        return new RingbufferMergeDataReadOnlyIterator<Object>(this);
    }
}
