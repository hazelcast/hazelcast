/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.jet.impl.util.FlatMappingTraverser;

import javax.annotation.Nonnull;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Traverses a potentially infinite sequence of non-{@code null} items. Each
 * invocation of {@link #next()} consumes and returns the next item in the
 * sequence if it is available, or returns {@code null} if not. An item may
 * still become available later on.
 * <p>
 * An important special case is traversing a finite sequence. In this case the
 * {@code null} return value means "the sequence is exhausted" and all future
 * {@code next()} calls will return {@code null}.
 *
 * @param <T> traversed item type
 */
@FunctionalInterface
public interface Traverser<T> {
    /**
     * Returns the next item in the sequence, or {@code null} if the sequence is
     * already exhausted
     */
    T next();

    /**
     * Adds a mapping layer to this traverser. The returned traverser will emit
     * the results of applying the mapper function to this traverser's items.
     */
    @Nonnull
    default <R> Traverser<R> map(@Nonnull Function<? super T, ? extends R> mapper) {
        return () -> {
            final T t = next();
            return t != null ? mapper.apply(t) : null;
        };
    }

    /**
     * Adds a filtering layer to this traverser. The returned traverser will
     * emit the same items as this traverser, but only those that pass the
     * given predicate.
     */
    @Nonnull
    default Traverser<T> filter(@Nonnull Predicate<? super T> pred) {
        return () -> {
            for (T t; (t = next()) != null;) {
                if (pred.test(t)) {
                    return t;
                }
            }
            return null;
        };
    }

    /**
     * Returns a traverser which appends an additional item to this traverser
     * after it returns the first {@code null} value.
     */
    @Nonnull
    default Traverser<T> append(@Nonnull T item) {
        return new Traverser<T>() {
            T appendedItem = item;
            @Override
            public T next() {
                T t = Traverser.this.next();
                if (t == null) {
                    try {
                        return appendedItem;
                    } finally {
                        appendedItem = null;
                    }
                }
                return t;
            }
        };
    }

    /**
     * Returns a traverser which prepends an additional item in front of
     * all the items of this traverser.
     */
    @Nonnull
    default Traverser<T> prepend(@Nonnull T item) {
        return new Traverser<T>() {
            private boolean itemReturned;
            @Override
            public T next() {
                if (itemReturned) {
                    return Traverser.this.next();
                }
                itemReturned = true;
                return item;
            }
        };
    }

    /**
     * Adds a flat-mapping layer to this traverser. The returned traverser
     * will apply the given mapping function to each item retrieved from this
     * traverser, and will emit all the items from the resulting traverser(s).
     */
    @Nonnull
    default <R> Traverser<R> flatMap(@Nonnull Function<? super T, ? extends Traverser<? extends R>> mapper) {
        return new FlatMappingTraverser<>(this, mapper);
    }

    /**
     * Returns a traverser that will emit the same items as this traverser,
     * additionally passing each item to the supplied consumer. A {@code null}
     * return value is not passed to the action.
     */
    @Nonnull
    default Traverser<T> peek(@Nonnull Consumer<? super T> action) {
        return () -> {
            T t = next();
            if (t != null) {
                action.accept(t);
            }
            return t;
        };
    }

    /**
     * Returns a traverser that will emit the same items as this traverser and
     * will additionally run the supplied action first time this traverser
     * returns {@code null}.
     */
    @Nonnull
    default Traverser<T> onFirstNull(@Nonnull Runnable action) {
        return new Traverser<T>() {
            private boolean didRun;

            @Override
            public T next() {
                T t = Traverser.this.next();
                if (t == null && !didRun) {
                    action.run();
                    didRun = true;
                }
                return t;
            }
        };
    }

    /**
     * Returns a traverser over the supplied arguments (or item array).
     *
     * @param items the items to traverse over
     * @param <T> type of the items
     */
    @SafeVarargs
    static <T> Traverser<T> over(T... items) {
        return Traversers.traverseArray(items);
    }
}
