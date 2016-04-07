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

package com.hazelcast.internal.util.collection;

import com.hazelcast.util.concurrent.IdleStrategy;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * Multi producer single consumer queue. This queue has a configurable {@link IdleStrategy} so if there is nothing to take,
 * the thread can idle and eventually can do the more expensive blocking. The blocking is especially a concern for the putting
 * thread, because it needs to notify the blocked thread.
 *
 * This MPSCQueue is based on 2 stacks; so the items are put in a reverse order by the putting thread, and by the taking thread
 * they are reversed in order again so that the original ordering is restored. Using this approach, if there are multiple items
 * on the stack, the owning thread can take them all using a single cas. Once this is done, the owning thread can process them
 * one by one and doesn't need to content with the putting threads; reducing contention.
 *
 * @param <E>
 */
public final class MPSCQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private static final Node BLOCKED = new Node();
    private static final int INITIAL_ARRAY_SIZE = 512;

    private final AtomicReference<Node> putStackHead = new AtomicReference<Node>();

    private Thread owningThread;
    private final IdleStrategy idleStrategy;

    // will only be used by the owningThread.
    private Object[] array;
    private int arrayIndex = -1;

    public MPSCQueue(Thread owningThread, IdleStrategy idleStrategy) {
        this.owningThread = owningThread;
        this.array = new Object[INITIAL_ARRAY_SIZE];
        this.idleStrategy = idleStrategy;
    }

    public void setOwningThread(Thread owningThread) {
        this.owningThread = owningThread;
    }

    @Override
    public void clear() {
        putStackHead.set(null);
    }

    @Override
    public boolean offer(E value) {
        checkNotNull(value,"value can't be null");

        AtomicReference<Node> head = putStackHead;
        Node newHead = new Node();
        newHead.value = value;

        for (; ; ) {
            Node oldHead = head.get();
            if (oldHead == null || oldHead == BLOCKED) {
                newHead.next = null;
                newHead.size = 1;
            } else {
                newHead.next = oldHead;
                newHead.size = oldHead.size + 1;
            }

            if (!head.compareAndSet(oldHead, newHead)) {
                continue;
            }

            if (oldHead == BLOCKED) {
                unpark(owningThread);
            }

            return true;
        }
    }

    @Override
    public E take() throws InterruptedException {
        E item = next();
        if (item != null) {
            return item;
        }

        takeAll();
        assert arrayIndex == 0;
        assert array[arrayIndex] != null;

        return next();
    }

    @Override
    public E poll() {
        E item = next();

        if (item != null) {
            return item;
        }

        if (!pollAll()) {
            return null;
        }

        return next();
    }

    private E next() {
        if (arrayIndex == -1) {
            return null;
        }

        if (arrayIndex == array.length) {
            arrayIndex = -1;
            return null;
        }

        E item = (E) array[arrayIndex];
        if (item == null) {
            arrayIndex = -1;
            return null;
        }
        array[arrayIndex] = null;
        arrayIndex++;
        return item;
    }

    public void takeAll() throws InterruptedException {
        long iteration = 0;
        AtomicReference<Node> head = putStackHead;
        for (; ; ) {
            if (owningThread.isInterrupted()) {
                head.compareAndSet(BLOCKED, null);
                throw new InterruptedException();
            }

            Node currentHead = head.get();

            if (currentHead == null) {
                // first we try to idle;
                if (!idleStrategy.idle(iteration)) {
                    // we don't need to block yet; so lets try again.
                    continue;
                }

                // there is nothing to be take, so lets block.
                if (!head.compareAndSet(null, BLOCKED)) {
                    continue;
                }

                park();
            } else if (currentHead == BLOCKED) {
                park();
            } else {
                if (!head.compareAndSet(currentHead, null)) {
                    continue;
                }

                initArray(currentHead);
                break;
            }
            iteration++;
        }
    }

    private boolean pollAll() {
        AtomicReference<Node> head = putStackHead;
        for (; ; ) {
            Node headNode = head.get();
            if (headNode == null) {
                return false;
            }

            if (head.compareAndSet(headNode, null)) {
                initArray(headNode);
                return true;
            }
        }
    }

    private void initArray(Node head) {
        int size = head.size;

        assert head != BLOCKED;
        assert size != 0;

        Object[] drain = this.array;
        if (size > drain.length) {
            drain = new Object[head.size * 2];
            this.array = drain;
        }

        for (int i = size - 1; i >= 0; i--) {
            drain[i] = head.value;
            head = head.next;
        }

        for (int k = 0; k < array.length; k++) {
            if (array[k] == null) {
                break;
            }
        }

        arrayIndex = 0;
        assert array[0] != null;
    }

    @Override
    public int size() {
        //todo: size can't be relied upon because this items which have been copied into the array, are not visible apart
        //to the owning thread.
        Node h = putStackHead.get();
        return h == null ? 0 : h.size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
        //  throw new UnsupportedOperationException();
    }

    @Override
    public void put(E e) throws InterruptedException {
        offer(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public E peek() {
        throw new UnsupportedOperationException();
    }

    private static final class Node<E> {
        Node next;
        E value;
        int size;
    }
}
