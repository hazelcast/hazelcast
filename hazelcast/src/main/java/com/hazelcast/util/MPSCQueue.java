/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * Single consumer, multi producer variable length queue implementation.
 * <p/>
 * The fast queue is a blocking implementation.
 *
 * @param <E>
 */
public final class MPSCQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private static final Node BLOCKED = new Node();
    private static final int INITIAL_ARRAY_SIZE = 512;

    private final Thread owningThread;
    private final PaddedAtomicReference<Node> head = new PaddedAtomicReference<Node>();
    private final boolean spin;
    private Object[] array;
    private int index = -1;

    public MPSCQueue(Thread owningThread) {
        this(owningThread, false);
    }

    public MPSCQueue(Thread owningThread, boolean spin) {
//        if (owningThread == null) {
//            throw new IllegalArgumentException("owningThread can't be null");
//        }
        this.owningThread = owningThread;
        this.array = new Object[INITIAL_ARRAY_SIZE];
        this.spin = spin;
    }

    @Override
    public void clear() {
        head.set(null);
    }

    @Override
    public boolean offer(E value) {
        if (value == null) {
            throw new IllegalArgumentException("value can't be null");
        }

        PaddedAtomicReference<Node> head = this.head;
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
        if (spin) {
            for (; ; ) {
                E item = poll();
                if (item != null) {
                    return item;
                }
            }
        }

        E item = next();
        if (item != null) {
            return item;
        }

        takeAll();
        assert index == 0;
        assert array[index] != null;

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
        if (index == -1) {
            return null;
        }

        if (index == array.length) {
            index = -1;
            return null;
        }

        E item = (E) array[index];
        if (item == null) {
            index = -1;
            return null;
        }
        array[index] = null;
        index++;
        return item;
    }

    public void takeAll() throws InterruptedException {
        PaddedAtomicReference<Node> head = this.head;
        for (; ; ) {
            Node currentHead = head.get();

            if (currentHead == null) {

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
        }

        if (owningThread.isInterrupted()) {
            head.compareAndSet(BLOCKED, null);
            throw new InterruptedException();
        }
    }

    public boolean pollAll() {
        PaddedAtomicReference<Node> head = this.head;
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

        index = 0;
        assert array[0] != null;
    }

    @Override
    public int size() {
        Node h = head.get();
        return h == null ? 0 : h.size;
    }

    @Override
    public boolean isEmpty() {
        return head.get() == null;
    }

    @Override
    public void put(E e) throws InterruptedException {
        throw new UnsupportedOperationException();
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
