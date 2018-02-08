/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.SplitBrainMergeEntryView;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.spi.impl.merge.SplitBrainEntryViews.createSplitBrainMergeEntryView;

/**
 * The ArrayRingbuffer is responsible for storing the actual content of a ringbuffer.
 * <p>
 * Currently the Ringbuffer is not a partitioned data-structure. So all data of a ringbuffer is stored in a single partition
 * and replicated to the replica's. No thread-safety is needed since a partition can only be accessed by a single thread at
 * any given moment.
 * <p>
 * The ringItems is the ring that contains the actual items.
 *
 * @param <E> the type of the data stored in the ringbuffer
 */
public class ArrayRingbuffer<E> implements Ringbuffer<E> {

    // contains the actual items
    E[] ringItems;

    private long tailSequence = -1;
    private long headSequence = tailSequence + 1;
    private int capacity;

    private SerializationService serializationService;

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
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    public long merge(SplitBrainMergeEntryView<Long, E> mergingEntry, SplitBrainMergePolicy mergePolicy, long remainingCapacity) {
        mergePolicy.setSerializationService(serializationService);

        // try to find an existing item with the same value
        E existingItem = null;
        long existingSequence = -1;
        for (long sequence = headSequence; sequence <= tailSequence; sequence++) {
            E item = read(sequence);
            if (mergingEntry.getValue().equals(item)) {
                existingItem = item;
                existingSequence = sequence;
                break;
            }
        }

        if (existingItem == null) {
            // if there is no capacity for another item, we don't merge
            if (remainingCapacity < 1) {
                return -1L;
            }

            E newValue = mergePolicy.merge(mergingEntry, null);
            if (newValue != null) {
                return add(newValue);
            }
        } else {
            SplitBrainMergeEntryView<Long, E> existingEntry = createSplitBrainMergeEntryView(existingSequence, existingItem);
            E newValue = mergePolicy.merge(mergingEntry, existingEntry);
            if (newValue != null && !newValue.equals(existingItem)) {
                set(existingSequence, newValue);
                return existingSequence;
            }
        }
        return -1L;
    }
}
