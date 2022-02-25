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

package com.hazelcast.internal.util.concurrent;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * Multi producer single consumer queue. This queue has a configurable {@link IdleStrategy} so if there is nothing to take,
 * the thread can idle and eventually can do the more expensive blocking. The blocking is especially a concern for the putting
 * thread, because it needs to notify the blocked thread.
 * <p>
 * This MPSCQueue is based on 2 stacks; so the items are put in a reverse order by the putting thread, and by the taking thread
 * they are reversed in order again so that the original ordering is restored. Using this approach, if there are multiple items
 * on the stack, the owning thread can take them all using a single CAS. Once this is done, the owning thread can process them
 * one by one and doesn't need to contend with the putting threads; reducing contention.
 *
 * @param <E> the type of elements held in this collection
 */
public final class MPSCQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    static final int INITIAL_ARRAY_SIZE = 512;
    static final Node BLOCKED = new Node();

    final AtomicReference<Node> putStack = new AtomicReference<>();
    private final AtomicInteger takeStackSize = new AtomicInteger();
    private final IdleStrategy idleStrategy;

    private Thread consumerThread;
    private Object[] takeStack = new Object[INITIAL_ARRAY_SIZE];
    private int takeStackIndex = -1;

    /**
     * Creates a new {@link MPSCQueue} with the provided {@link IdleStrategy} and consumer thread.
     *
     * @param consumerThread the Thread that consumes the items.
     * @param idleStrategy   the idleStrategy. If null, this consumer will block if the queue is empty.
     * @throws NullPointerException when consumerThread is null.
     */
    public MPSCQueue(Thread consumerThread, IdleStrategy idleStrategy) {
        this.consumerThread = checkNotNull(consumerThread, "consumerThread can't be null");
        this.idleStrategy = idleStrategy;
    }

    /**
     * Creates a new {@link MPSCQueue} with the provided {@link IdleStrategy}.
     *
     * @param idleStrategy the idleStrategy. If null, the consumer will block.
     */
    public MPSCQueue(IdleStrategy idleStrategy) {
        this.idleStrategy = idleStrategy;
    }

    /**
     * Sets the consumer thread.
     *
     * The consumer thread is needed for blocking, so that an offering known which thread
     * to wakeup. There can only be a single consumerThread and this method should be called
     * before the queue is safely published. It will not provide a happens before relation on
     * its own.
     *
     * @param consumerThread the consumer thread.
     * @throws NullPointerException when consumerThread null.
     */
    public void setConsumerThread(Thread consumerThread) {
        this.consumerThread = checkNotNull(consumerThread, "consumerThread can't be null");
    }

    /**
     * {@inheritDoc}.
     *
     * This call is threadsafe; but it will only remove the items that are on the put-stack.
     */
    @Override
    public void clear() {
        putStack.set(BLOCKED);
    }

    @Override
    public boolean offer(E item) {
        checkNotNull(item, "item can't be null");

        AtomicReference<Node> putStack = this.putStack;
        Node newHead = new Node();
        newHead.item = item;

        for (; ; ) {
            Node oldHead = putStack.get();
            if (oldHead == null || oldHead == BLOCKED) {
                newHead.next = null;
                newHead.size = 1;
            } else {
                newHead.next = oldHead;
                newHead.size = oldHead.size + 1;
            }

            if (!putStack.compareAndSet(oldHead, newHead)) {
                continue;
            }

            if (oldHead == BLOCKED) {
                unpark(consumerThread);
            }

            return true;
        }
    }

    @Override
    public E peek() {
        E item = peekNext();
        if (item != null) {
            return item;
        }
        if (!drainPutStack()) {
            return null;
        }
        return peekNext();
    }

    @Override
    public E take() throws InterruptedException {
        E item = next();
        if (item != null) {
            return item;
        }

        takeAll();
        assert takeStackIndex == 0;
        assert takeStack[takeStackIndex] != null;

        return next();
    }

    @Override
    public E poll() {
        E item = next();

        if (item != null) {
            return item;
        }

        if (!drainPutStack()) {
            return null;
        }

        return next();
    }

    private E next() {
        E item = peekNext();
        if (item != null) {
            dequeue();
        }
        return item;
    }

    private E peekNext() {
        if (takeStackIndex == -1) {
            return null;
        }

        if (takeStackIndex == takeStack.length) {
            takeStackIndex = -1;
            return null;
        }

        E item = (E) takeStack[takeStackIndex];
        if (item == null) {
            takeStackIndex = -1;
            return null;
        }
        return item;
    }

    private void dequeue() {
        takeStack[takeStackIndex] = null;
        takeStackIndex++;
        takeStackSize.lazySet(takeStackSize.get() - 1);
    }

    private void takeAll() throws InterruptedException {
        long iteration = 0;
        AtomicReference<Node> putStack = this.putStack;
        for (; ; ) {
            if (consumerThread.isInterrupted()) {
                putStack.compareAndSet(BLOCKED, null);
                throw new InterruptedException();
            }

            Node currentPutStackHead = putStack.get();

            if (currentPutStackHead == null) {
                if (idleStrategy != null) {
                    idleStrategy.idle(iteration);
                    continue;
                }

                // there is nothing to be take, so lets block.
                if (!putStack.compareAndSet(null, BLOCKED)) {
                    // we are lucky, something is available
                    continue;
                }

                // lets block for real.
                park();
            } else if (currentPutStackHead == BLOCKED) {
                park();
            } else {
                if (!putStack.compareAndSet(currentPutStackHead, null)) {
                    continue;
                }

                copyIntoTakeStack(currentPutStackHead);
                break;
            }
            iteration++;
        }
    }

    private boolean drainPutStack() {
        for (; ; ) {
            Node head = putStack.get();
            if (head == null) {
                return false;
            }

            if (putStack.compareAndSet(head, null)) {
                copyIntoTakeStack(head);
                return true;
            }
        }
    }

    private void copyIntoTakeStack(Node putStackHead) {
        int putStackSize = putStackHead.size;

        takeStackSize.lazySet(putStackSize);

        if (putStackSize > takeStack.length) {
            takeStack = new Object[nextPowerOfTwo(putStackHead.size)];
        }

        for (int i = putStackSize - 1; i >= 0; i--) {
            takeStack[i] = putStackHead.item;
            putStackHead = putStackHead.next;
        }

        takeStackIndex = 0;
        assert takeStack[0] != null;
    }

    /**
     * {@inheritDoc}.
     *
     * Best effort implementation.
     */
    @Override
    public int size() {
        Node h = putStack.get();
        int putStackSize = h == null ? 0 : h.size;
        return putStackSize + takeStackSize.get();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void put(E e) {
        offer(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) {
        add(e);
        return true;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
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

    private static final class Node<E> {
        Node next;
        E item;
        int size;
    }
}
