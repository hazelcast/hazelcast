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
 * The RingbufferContainer is responsible for storing the actual content of a ringbuffer.
 * <p/>
 * Currently the Ringbuffer is not a partitioned data-structure. So all data of a ringbuffer is stored in a single partition
 * and replicated to the replica's. No thread-safety is needed since a partition can only be accessed by a single thread at
 * any given moment.
 * <p/>
 * The ringItems is the ring that contains the actual items.
 */
public class ArrayRingbuffer implements Ringbuffer {
    // contains the actual items
    Object[] ringItems;
    private long tailSequence = -1;
    private long headSequence = tailSequence + 1;
    private int capacity;

    public ArrayRingbuffer(int capacity) {
        this.capacity = capacity;
        this.ringItems = new Object[capacity];
    }

    @Override
    public long tailSequence() {
        return tailSequence;
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
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public long add(Object item) {
        tailSequence++;

        if (tailSequence - capacity == headSequence) {
            headSequence++;
        }

        int index = toIndex(tailSequence);

        ringItems[index] = item;

        return tailSequence;
    }


    @Override
    public Object read(long sequence) {
        checkReadSequence(sequence);
        return ringItems[toIndex(sequence)];
    }

    @Override
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
    public void set(long seq, Object data) {
        ringItems[toIndex(seq)] = data;
    }
}

