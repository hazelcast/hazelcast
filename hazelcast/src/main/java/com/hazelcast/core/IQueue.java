/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.core;

import com.hazelcast.monitor.LocalQueueStats;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Concurrent, blocking, distributed, observable queue.
 *
 * Queues are stored in one node in the cluster with one backup.
 *
 * @see BaseQueue
 * @see java.util.Queue
 * @see BlockingQueue
 * @see TransactionalQueue
 * @param <E>
 */
public interface IQueue<E> extends BlockingQueue<E>, BaseQueue<E>, ICollection<E> {

    /**
     * {@inheritDoc}
     */
    boolean add(E e);

    /**
     * {@inheritDoc}
     */
    boolean offer(E e);

    /**
     * {@inheritDoc}
     */
    void put(E e) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    E take() throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    E poll(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    int remainingCapacity();

    /**
     * {@inheritDoc}
     */
    boolean remove(Object o);

    /**
     * {@inheritDoc}
     */
    boolean contains(Object o);

    /**
     * {@inheritDoc}
     */
    int drainTo(Collection<? super E> c);

    /**
     * {@inheritDoc}
     */
    int drainTo(Collection<? super E> c, int maxElements);

    /**
     * {@inheritDoc}
     */
    E remove();

    /**
     * {@inheritDoc}
     */
    E poll();

    /**
     * {@inheritDoc}
     */
    E element();

    /**
     * {@inheritDoc}
     */
    E peek();

    /**
     * {@inheritDoc}
     */
    int size();

    /**
     * {@inheritDoc}
     */
    boolean isEmpty();

    /**
     * {@inheritDoc}
     *
     * <p> The view's <tt>iterator</tt> is a "weakly consistent" iterator,
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     */
    Iterator<E> iterator();

    /**
     * {@inheritDoc}
     */
    Object[] toArray();

    /**
     * {@inheritDoc}
     */
    <T> T[] toArray(T[] a);

    /**
     * {@inheritDoc}
     */
    boolean containsAll(Collection<?> c);

    /**
     * {@inheritDoc}
     */
    boolean addAll(Collection<? extends E> c);

    /**
     * {@inheritDoc}
     */
    boolean removeAll(Collection<?> c);

    /**
     * {@inheritDoc}
     */
    boolean retainAll(Collection<?> c);

    /**
     * {@inheritDoc}
     */
    void clear();

    /**
     * Returns LocalQueueStats for this queue.
     * LocalQueueStats is the statistics for the local portion of this
     * queue.
     *
     * @return this queue's local statistics.
     */
    LocalQueueStats getLocalQueueStats();
}
