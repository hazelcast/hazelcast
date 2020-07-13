/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.Data;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A {@link PriorityQueue} which did not allowed duplicate values.
 * Duplicate check is not done on QueueItem but on {@link QueueItem#getData()}
 */
@BinaryInterface
public final class NoDuplicatePriorityQueue extends PriorityQueue<QueueItem> {

    private Set<Data> dataSet = new HashSet();

    /**
     * Constructs an instance of {@code NoDuplicatePriorityQueue}
     *
     * @param comparator supplied comparator to be used by this priority queue
     */
    public NoDuplicatePriorityQueue(Comparator<QueueItem> comparator) {
        super(comparator);
    }

    @Override
    public boolean offer(QueueItem e) {
        Data otherData = e.getData();
        if (dataSet.contains(otherData)) {
            return false;
        }
        boolean added = super.offer(e);
        if (added) {
            dataSet.add(otherData);
        }
        return added;
    }

    @Override
    public QueueItem poll() {
        final QueueItem element = super.poll();
        if (element != null) {
            dataSet.remove(element.getData());
        }
        return element;
    }

    @Override
    public boolean add(QueueItem e) {
        return this.offer(e);
    }

    @Override
    public boolean remove(Object o) {
        Data otherData = ((QueueItem) o).getData();
        boolean removed = super.remove(o);
        if (removed) {
            dataSet.remove(otherData);
        }
        return removed;
    }
}
