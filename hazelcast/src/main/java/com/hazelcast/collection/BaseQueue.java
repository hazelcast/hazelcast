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

package com.hazelcast.collection;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.transaction.TransactionalQueue;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 *
 * Base interface for Hazelcast distributed queues.
 *
 * @see java.util.concurrent.BlockingQueue
 * @see IQueue
 * @see TransactionalQueue
 * @param <E> queue item type
 */
public interface BaseQueue<E> extends DistributedObject {

    /**
     * Inserts the specified element into this queue if it is possible to do
     * so immediately without violating capacity restrictions. Returns
     * <code>true</code> upon success and <code>false</code> if no space is currently
     * available.
     *
     * @param e the element to add
     * @return <code>true</code> if the element was added to this queue,
     *         <code>false</code> otherwise
     */
    boolean offer(@Nonnull E e);

    /**
     * Inserts the specified element into this queue, waiting up to the
     * specified wait time if necessary for space to become available.
     *
     * @param e the element to add
     * @param timeout how long to wait before giving up, in units of
     *        <code>unit</code>
     * @param unit a <code>TimeUnit</code> determines how to interpret the
     *        <code>timeout</code> parameter
     * @return <code>true</code> if successful, or <code>false</code> if
     *         the specified waiting time elapses before space is available
     * @throws InterruptedException if interrupted while waiting
     */
    boolean offer(@Nonnull E e, long timeout, @Nonnull TimeUnit unit) throws InterruptedException;

    /**
     * Retrieves and removes the head of this queue, waiting if necessary
     * until an element becomes available.
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    @Nonnull E take() throws InterruptedException;

    /**
     * Retrieves and removes the head of this queue,
     * or returns <code>null</code> if this queue is empty.
     *
     * @return the head of this queue, or <code>null</code> if this queue is empty
     */
    E poll();

    /**
     * Retrieves and removes the head of this queue, waiting up to the
     * specified wait time if necessary for an element to become available.
     *
     * @param timeout how long to wait before giving up, in units of
     *        <code>unit</code>
     * @param unit a <code>TimeUnit</code> determining how to interpret the
     *        <code>timeout</code> parameter
     * @return the head of this queue, or <code>null</code> if the
     *         specified waiting time elapses before an element is available
     * @throws InterruptedException if interrupted while waiting
     */
    E poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException;

    /**
     * Returns the number of elements in this collection.  If this collection
     * contains more than <code>Integer.MAX_VALUE</code> elements, returns
     * <code>Integer.MAX_VALUE</code>.
     *
     * @return the number of elements in this collection
     */
    int size();

}
