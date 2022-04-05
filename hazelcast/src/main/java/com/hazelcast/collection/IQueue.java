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

import com.hazelcast.transaction.TransactionalQueue;

import javax.annotation.Nonnull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Concurrent, blocking, distributed, observable queue.
 * <p>
 * The IQueue is not a partitioned data-structure. All the content of an
 * IQueue is stored in a single machine (and in the backup).
 * The IQueue will not scale by adding more members to the cluster.
 *
 * @param <E> queue item type
 * @see BaseQueue
 * @see java.util.Queue
 * @see BlockingQueue
 * @see TransactionalQueue
 */
public interface IQueue<E> extends BlockingQueue<E>, BaseQueue<E>, ICollection<E> {
    /*
     * Added poll(), poll(long timeout, TimeUnit unit) and take()
     * methods here to prevent wrong method return type issue when
     * compiled with java 8.
     *
     * For additional details see:
     *
     * http://mail.openjdk.java.net/pipermail/compiler-dev/2014-November/009139.html
     * https://bugs.openjdk.java.net/browse/JDK-8064803
     *
     */

    /**
     * {@inheritDoc}
     */
    E poll();

    /**
     * {@inheritDoc}
     */
    E poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException;

    /**
     * {@inheritDoc}
     */
    @Nonnull
    E take() throws InterruptedException;

    /**
     * Returns LocalQueueStats for this queue.
     * LocalQueueStats is the statistics for the local portion of this
     * queue.
     *
     * @return this queue's local statistics.
     */
    LocalQueueStats getLocalQueueStats();
}
