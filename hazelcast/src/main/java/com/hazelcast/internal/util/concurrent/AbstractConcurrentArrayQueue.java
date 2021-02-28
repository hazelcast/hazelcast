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

import com.hazelcast.internal.util.QuickMath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;

/** Pad out a cacheline to the left of producer fields to prevent false sharing. */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
class AbstractConcurrentArrayQueuePadding1 {
    @SuppressWarnings({"unused", "checkstyle:multiplevariabledeclarations"})
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

/** Values for the producer that are expected to be padded. */
class AbstractConcurrentArrayQueueProducer extends AbstractConcurrentArrayQueuePadding1 {
    protected static final AtomicLongFieldUpdater<AbstractConcurrentArrayQueueProducer> TAIL =
            AtomicLongFieldUpdater.newUpdater(AbstractConcurrentArrayQueueProducer.class, "tail");
    protected static final AtomicLongFieldUpdater<AbstractConcurrentArrayQueueProducer> SHARED_HEAD_CACHE =
            AtomicLongFieldUpdater.newUpdater(AbstractConcurrentArrayQueueProducer.class, "sharedHeadCache");

    protected volatile long tail;
    protected long headCache;
    protected volatile long sharedHeadCache;
}

/** Pad out a cacheline between the producer and consumer fields to prevent false sharing. */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
class AbstractConcurrentArrayQueuePadding2 extends AbstractConcurrentArrayQueueProducer {
    @SuppressWarnings({"unused", "checkstyle:multiplevariabledeclarations"})
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

/** Values for the consumer that are expected to be padded. */
class AbstractConcurrentArrayQueueConsumer extends AbstractConcurrentArrayQueuePadding2 {
    protected static final AtomicLongFieldUpdater<AbstractConcurrentArrayQueueConsumer> HEAD =
            AtomicLongFieldUpdater.newUpdater(AbstractConcurrentArrayQueueConsumer.class, "head");

    protected volatile long head;
}

/** Pad out a cacheline to the right of consumer fields to prevent false sharing. */
@SuppressFBWarnings(value = "UuF", justification = "Fields used for padding are unused programatically")
class AbstractConcurrentArrayQueuePadding3 extends AbstractConcurrentArrayQueueConsumer {
    @SuppressWarnings({"unused", "checkstyle:multiplevariabledeclarations"})
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;
}

/**
 * Abstract base class for concurrent array queues.
 * @param <E> type of elements in the queue.
 */
abstract class AbstractConcurrentArrayQueue<E>
extends AbstractConcurrentArrayQueuePadding3
implements QueuedPipe<E> {

    protected final int capacity;
    protected final AtomicReferenceArray<E> buffer;

    @SuppressWarnings("unchecked")
    protected AbstractConcurrentArrayQueue(int requestedCapacity) {
        capacity = QuickMath.nextPowerOfTwo(requestedCapacity);
        buffer = new AtomicReferenceArray(capacity);
    }

    @Override
    public long addedCount() {
        return tail;
    }

    @Override
    public long removedCount() {
        return head;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int remainingCapacity() {
        return capacity() - size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public E peek() {
        return buffer.get(seqToArrayIndex(head, capacity - 1));
    }

    @Override
    public boolean add(E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full");
    }

    @Override
    public E remove() {
        final E e = poll();
        if (e == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        return e;
    }

    @Override
    public E element() {
        final E e = peek();
        if (e == null) {
            throw new NoSuchElementException("Queue is empty");
        }
        return e;
    }

    @Override
    public boolean isEmpty() {
        return peek() == null;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        }
        final AtomicReferenceArray<E> buffer = this.buffer;
        long mask = capacity - 1;
        for (long i = head, limit = tail; i < limit; i++) {
            final Object e = buffer.get(seqToArrayIndex(i, mask));
            if (o.equals(e)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (final Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        for (E e : c) {
            add(e);
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        Object value;
        do {
            value = poll();
        } while (value != null);
    }

    @Override
    public int size() {
        long currentHeadBefore;
        long currentTail;
        long currentHeadAfter = head;
        do {
            currentHeadBefore = currentHeadAfter;
            currentTail = tail;
            currentHeadAfter = head;
        } while (currentHeadAfter != currentHeadBefore);
        return (int) (currentTail - currentHeadAfter);
    }

    protected static int seqToArrayIndex(long sequence, long mask) {
        return (int) (sequence & mask);
    }
}
