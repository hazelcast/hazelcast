/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.concurrent;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;

/**
 * Many producers to single consumer concurrent queue backed by an array.
 * Adapted from the Agrona project.
 *
 * @param <E> type of the elements stored in the queue.
 */
public class ManyToOneConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> {

    public ManyToOneConcurrentArrayQueue(int requestedCapacity) {
        super(requestedCapacity);
    }

    @Override
    public boolean offer(E e) {
        assert e != null : "attempt to offer a null element";

        final int capacity = this.capacity;

        long acquiredHead = sharedHeadCache;
        long bufferLimit = acquiredHead + capacity;
        long acquiredTail;
        do {
            acquiredTail = tail;
            if (acquiredTail >= bufferLimit) {
                acquiredHead = head;
                bufferLimit = acquiredHead + capacity;
                if (acquiredTail >= bufferLimit) {
                    return false;
                }
                SHARED_HEAD_CACHE.lazySet(this, acquiredHead);
            }
        } while (!TAIL.compareAndSet(this, acquiredTail, acquiredTail + 1));
        buffer.lazySet(seqToArrayIndex(acquiredTail, capacity - 1), e);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public E poll() {
        final long head = this.head;
        final AtomicReferenceArray<E> buffer = this.buffer;
        final int arrayIndex = seqToArrayIndex(head, capacity - 1);
        final E item = buffer.get(arrayIndex);
        if (item != null) {
            buffer.lazySet(arrayIndex, null);
            HEAD.lazySet(this, head + 1);
        }
        return item;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int drain(Predicate<? super E> itemHandler) {
        final AtomicReferenceArray<E> buffer = this.buffer;
        final long mask = capacity - 1;
        final long acquiredHead = head;
        final long limit = acquiredHead + mask + 1;

        long nextSequence = acquiredHead;
        while (nextSequence < limit) {
            final int arrayIndex = seqToArrayIndex(nextSequence, mask);
            final E item = buffer.get(arrayIndex);
            if (item == null) {
                break;
            }
            buffer.lazySet(arrayIndex, null);
            nextSequence++;
            HEAD.lazySet(this, nextSequence);
            if (!itemHandler.test(item)) {
                break;
            }
        }
        return (int) (nextSequence - acquiredHead);
    }

    @SuppressWarnings("unchecked")
    @Override
    public int drainTo(Collection<? super E> target, int limit) {
        final AtomicReferenceArray<E> buffer = this.buffer;
        final long mask = capacity - 1;

        long nextSequence = head;
        int count = 0;
        while (count < limit) {
            final int arrayIndex = seqToArrayIndex(nextSequence, mask);
            final E item = buffer.get(arrayIndex);
            if (item == null) {
                break;
            }
            buffer.lazySet(arrayIndex, null);
            nextSequence++;
            HEAD.lazySet(this, nextSequence);
            count++;
            target.add(item);
        }
        return count;
    }
}
