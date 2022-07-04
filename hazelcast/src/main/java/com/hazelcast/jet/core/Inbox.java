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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A subset of {@code Queue<Object>} API restricted to the consumer side,
 * with additional support for bulk draining operations.
 *
 * @since Jet 3.0
 */
public interface Inbox extends Iterable<Object> {

    /**
     * Returns {@code true} if this inbox contains no elements, {@code false} otherwise.
     */
    boolean isEmpty();

    /**
     * Retrieves, but does not remove, the head of this inbox, or returns
     * {@code null} if it is empty.
     */
    @Nullable
    Object peek();

    /**
     * Retrieves and removes the head of this inbox, or returns {@code null}
     * if it is empty.
     */
    @Nullable
    Object poll();

    /**
     * Removes the head of this inbox. This method throws an exception
     * if the inbox is empty.
     *
     * @throws NoSuchElementException if this inbox is empty
     */
    void remove();

    /**
     * Returns an iterator over the items in the inbox in the order they would
     * be returned by the {@link #poll()} method.
     * <p>
     * The returned iterator doesn't support the {@link Iterator#remove()}
     * method.
     */
    @Override @Nonnull
    Iterator<Object> iterator();

    /**
     * Removes all items from the inbox.
     */
    void clear();

    /**
     * Drains all elements into the provided {@link Collection}.
     *
     * @param target the collection to drain this object's items into
     * @return the number of elements actually drained
     */
    default <E> int drainTo(Collection<E> target) {
        return drainTo(target, Integer.MAX_VALUE);
    }

    /**
     * Drains at most {@code limit} elements into the provided {@link
     * Collection}.
     *
     * @param target the collection to drain this object's items into
     * @param limit the maximum amount of items to drain
     * @return the number of elements actually drained
     *
     * @since Jet 4.0
     */
    @SuppressWarnings("unchecked")
    default <E> int drainTo(@Nonnull Collection<E> target, int limit) {
        int drained = 0;
        for (E o; drained < limit && (o = (E) poll()) != null; drained++) {
            target.add(o);
        }
        return drained;
    }

    /**
     * Drains and maps at most {@code limit} elements into the provided
     * {@link Collection}.
     *
     * @param target the collection to drain this object's items into
     * @param limit the maximum amount of items to drain
     * @param mapper mapping function to apply to this object's items
     * @return the number of elements actually drained
     *
     * @since Jet 4.1
     */
    @SuppressWarnings("unchecked")
    default <E, M> int drainTo(@Nonnull Collection<M> target, int limit, @Nonnull Function<E, M> mapper) {
        int drained = 0;
        for (E o; drained < limit && (o = (E) poll()) != null; drained++) {
            target.add(mapper.apply(o));
        }
        return drained;
    }

    /**
     * Passes each of this object's items to the supplied consumer until it is empty.
     *
     * @return the number of elements drained
     */
    @SuppressWarnings("unchecked")
    default <E> int drain(@Nonnull Consumer<E> consumer) {
        for (Object o : this) {
            consumer.accept((E) o);
        }
        int consumed = size();
        clear();
        return consumed;
    }

    /**
     * Returns the number of objects in the inbox.
     */
    int size();
}
