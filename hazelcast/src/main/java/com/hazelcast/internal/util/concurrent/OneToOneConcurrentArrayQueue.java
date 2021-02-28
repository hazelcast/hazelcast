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
 * Single producer to single consumer concurrent queue backed by an array.
 * Adapted from the Agrona project.
 *
 * @param <E> type of the elements stored in the queue.
 */
public class OneToOneConcurrentArrayQueue<E> extends AbstractConcurrentArrayQueue<E> {

    public OneToOneConcurrentArrayQueue(final int requestedCapacity) {
        super(requestedCapacity);
    }

    @Override
    public boolean offer(final E e) {
        assert e != null : "Attempted to offer null to a concurrent array queue";

        final int capacity = this.capacity;
        final long currentTail = tail;

        long acquiredHead = headCache;
        long bufferLimit = acquiredHead + capacity;
        if (currentTail >= bufferLimit) {
            acquiredHead = head;
            bufferLimit = acquiredHead + capacity;
            if (currentTail >= bufferLimit) {
                return false;
            }
            headCache = acquiredHead;
        }
        final int arrayIndex = seqToArrayIndex(currentTail, capacity - 1);
        buffer.lazySet(arrayIndex, e);
        TAIL.lazySet(this, currentTail + 1);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public E poll() {
        final AtomicReferenceArray<E> buffer = this.buffer;
        final long currentHead = head;
        final int arrayIndex = seqToArrayIndex(currentHead, capacity - 1);
        final E e = buffer.get(arrayIndex);
        if (e != null) {
            buffer.lazySet(arrayIndex, null);
            HEAD.lazySet(this, currentHead + 1);
        }
        return e;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int drain(Predicate<? super E> itemHandler) {
        final AtomicReferenceArray<E> buffer = this.buffer;
        final long mask = this.capacity - 1;
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

    @Override
    @SuppressWarnings("unchecked")
    public int drainTo(final Collection<? super E> target, final int limit) {
        if (limit <= 0) {
            return 0;
        }
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
