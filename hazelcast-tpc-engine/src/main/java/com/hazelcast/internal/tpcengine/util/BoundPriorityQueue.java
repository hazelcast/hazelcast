/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import com.hazelcast.internal.serialization.SerializableByConvention;

import java.util.PriorityQueue;

/**
 * A bound version of the {@link PriorityQueue}. Instead of having the backing
 * array grow, the array has a fixed size. So no growing and control on the maximum
 * number of items in this priority queue.
 *
 * @param <E> the type of elements in this BoundPriorityQueue.
 */
@SerializableByConvention
public class BoundPriorityQueue<E> extends PriorityQueue<E> {

    private final int capacity;

    /**
     * Creates a BoundPriorityQueue with the given capacity.
     *
     * @param capacity the capacity.
     * @throws IllegalArgumentException when capacity smaller than 0.
     */
    public BoundPriorityQueue(int capacity) {
        super(capacity);
        this.capacity = capacity;
    }

    @Override
    public boolean offer(E e) {
        if (size() == capacity) {
            return false;
        }

        return super.offer(e);
    }
}
