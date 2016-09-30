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

package com.hazelcast.jet2.impl.queue;

import com.hazelcast.util.function.Consumer;

import java.util.Collection;

/**
 * A container for items processed in sequence.
 *
 * @param <E> type of elements in the pipe.
 */
public interface Pipe<E> {
    /**
     * The number of items added to this container since creation.
     *
     * @return the number of items added.
     */
    long addedCount();

    /**
     * The number of items removed from this container since creation.
     *
     * @return the number of items removed.
     */
    long removedCount();

    /**
     * The maximum capacity of this container to hold items.
     *
     * @return the capacity of the container.
     */
    int capacity();

    /**
     * Get the remaining capacity for elements in the container given the current size.
     *
     * @return remaining capacity of the container
     */
    int remainingCapacity();

    /**
     * Invoke a {@link Consumer} callback on each elements to drain the collection of elements until it is empty.
     *
     * If possible, implementations should use smart batching to best handle burst traffic.
     *
     * @param elementHandler to callback for processing elements
     * @return the number of elements drained
     */
    int drain(Consumer<E> elementHandler);

    /**
     * Drain available elements into the provided {@link Collection} up to a provided maximum limit of elements.
     *
     * If possible, implementations should use smart batching to best handle burst traffic.
     *
     * @param target in to which elements are drained.
     * @param limit  of the maximum number of elements to drain.
     * @return the number of elements actually drained.
     */
    int drainTo(Collection<? super E> target, int limit);
}
