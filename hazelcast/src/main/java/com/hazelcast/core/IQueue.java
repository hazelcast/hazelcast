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
 * The IQueue is not a partitioned data-structure. So all the content of the IQueue is stored in a single machine (and
 * in the backup). So the IQueue will not scale by adding more members in the cluster.
 *
 * @see BaseQueue
 * @see java.util.Queue
 * @see BlockingQueue
 * @see TransactionalQueue
 * @param <E>
 */
public interface IQueue<E> extends BlockingQueue<E>, BaseQueue<E>, ICollection<E> {

    /**
     * Returns LocalQueueStats for this queue.
     * LocalQueueStats is the statistics for the local portion of this
     * queue.
     *
     * @return this queue's local statistics.
     */
    LocalQueueStats getLocalQueueStats();
}
