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

package com.hazelcast.jet.core;

import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;

/**
 * A traverser with an internal {@link ArrayDeque}. You can efficiently {@link
 * #append} items to it that will be later traversed.
 * <p>
 * It's useful to be returned from a flat-mapping function when an item is
 * flat-mapped to a small number of items:
 * <pre>{@code
 *     Traverser<Integer> at = new AppendableTraverser(2);
 *     Traverser<Integer> t = Traverser.over(10, 20)
 *         .flatMap(item -> {
 *             at.append(item);
 *             at.append(item + 1);
 *             return at;
 *         });
 * }</pre>
 * The {@code t} traverser above will output {10, 11, 20, 21}. This approach
 * reduces the GC pressure by avoiding the allocation of a new traverser for
 * each item that will traverse over just a few or even zero items.
 * <p>
 * See {@link ResettableSingletonTraverser} if you have at most one item to
 * traverse.
 *
 * @param <T> item type
 *
 * @since Jet 3.0
 */
public class AppendableTraverser<T> implements Traverser<T> {
    private final ArrayDeque<T> queue;

    /**
     * Creates an appendable traverser.
     */
    public AppendableTraverser(int initialCapacity) {
        queue = new ArrayDeque<>(initialCapacity);
    }

    /**
     * {@inheritDoc}
     * @return {@code this} instance
     */
    @Nonnull @Override
    public AppendableTraverser<T> append(@Nonnull T item) {
        queue.add(item);
        return this;
    }

    @Override
    public T next() {
        return queue.poll();
    }

    /**
     * Returns {@code true}, if the next call to {@link #next} will return
     * {@code null}.
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
