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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Iterator;

/**
 * The ArrayRingbuffer is responsible for storing the actual contents of a
 * ringbuffer.
 * <p>
 * Currently the Ringbuffer is not a partitioned data-structure. So all
 * data of a ringbuffer is stored in a single partition and replicated to
 * the replicas. No thread safety is needed since a partition can only be
 * accessed by a single thread at any given moment.
 *
 * @param <E> the type of the data stored in the ringbuffer
 */
public class ArrayRingbuffer<E> implements Ringbuffer<E> {

    private E[] ringItems;
    private long tailSequence = -1;
    private long headSequence = tailSequence + 1;
    private int capacity;

    @SuppressWarnings("unchecked")
    public ArrayRingbuffer(int capacity) {
        this.capacity = capacity;
        this.ringItems = (E[]) new Object[capacity];
    }

    @Override
    public long tailSequence() {
        return tailSequence;
    }

    @Override
    public long peekNextTailSequence() {
        return tailSequence + 1;
    }

    @Override
    public void setTailSequence(long sequence) {
        this.tailSequence = sequence;
    }

    @Override
    public long headSequence() {
        return headSequence;
    }

    @Override
    public void setHeadSequence(long sequence) {
        this.headSequence = sequence;
    }

    @Override
    public long getCapacity() {
        return capacity;
    }

    @Override
    public long size() {
        return tailSequence - headSequence + 1;
    }

    @Override
    // not used in the codebase, here just for future API usage
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public long add(E item) {
        tailSequence++;

        if (tailSequence - capacity == headSequence) {
            headSequence++;
        }

        int index = toIndex(tailSequence);

        ringItems[index] = item;

        return tailSequence;
    }

    @Override
    public E read(long sequence) {
        checkReadSequence(sequence);
        return ringItems[toIndex(sequence)];
    }

    @Override
    // This method is not used since the RingbufferContainer also checks if the ring buffer store is enabled before
    // throwing a StaleSequenceException
    public void checkBlockableReadSequence(long readSequence) {
        if (readSequence > tailSequence + 1) {
            throw new IllegalArgumentException("sequence:" + readSequence
                    + " is too large. The current tailSequence is:" + tailSequence);
        }

        if (readSequence < headSequence) {
            throw new StaleSequenceException("sequence:" + readSequence
                    + " is too small. The current headSequence is:" + headSequence
                    + " tailSequence is:" + tailSequence, headSequence);
        }
    }

    @Override
    // the sequence is usually in the right range since the RingbufferContainer checks it too
    public void checkReadSequence(long sequence) {
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
        return (int) (sequence % ringItems.length);
    }

    @Override
    public void set(long seq, E data) {
        ringItems[toIndex(seq)] = data;
    }

    @Override
    public void clear() {
        Arrays.fill(ringItems, null);
        tailSequence = -1;
        headSequence = tailSequence + 1;
    }

    @Override
    public Iterator<E> iterator() {
        return new ReadOnlyRingbufferIterator<E>(this);
    }

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public E[] getItems() {
        return ringItems;
    }
}
