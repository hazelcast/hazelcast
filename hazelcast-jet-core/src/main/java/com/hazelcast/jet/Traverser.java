/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
     * Returns the next item in the sequence, or {@code null} if there is no next
     * item to return. If this traverser represents a finite sequence, it will
     * keep returning {@code null} once exhausted. If it represents an infinite
     * sequence, {@code null} only means "there is currently no next item", but
     * trying again later may produce one.
     */
    T next();

    /**
     * Returns a traverser that will emit the results of applying the
     * mapping function to this traverser's items.
     * <p>
     * If the {@code mapFn} returns {@code null}, that item will be filtered
     * out.
     */
    @Nonnull
    default <R> Traverser<R> map(@Nonnull Function<? super T, ? extends R> mapFn) {
        return () -> {
            for (T t; (t = next()) != null;) {
                R r = mapFn.apply(t);
                if (r != null) {
                    return r;
                }
            }
            return null;
        };
    }

    /**
     * Returns a traverser that will emit the same items as this traverser, but
     * only those that pass the given predicate.
     */
    @Nonnull
    default Traverser<T> filter(@Nonnull Predicate<? super T> filterFn) {
        return () -> {
            for (T t; (t = next()) != null;) {
                if (filterFn.test(t)) {
                    return t;
                }
            }
            return null;
        };
    }

    /**
     * Returns a traverser traverser that will apply the given mapping function
     * to each item retrieved from this traverser and emit all the items from
     * the resulting traverser(s).
     * <p>
     * The traverser returned from the {@code flatMapFn} must be finite. That
     * is, this operation will not attempt to emit any items after the first
     * {@code null} item.
     */
    @Nonnull
    default <R> Traverser<R> flatMap(@Nonnull Function<? super T, ? extends Traverser<? extends R>> flatMapFn) {
        return new FlatMappingTraverser<>(this, flatMapFn);
    }

    /**
     * Returns a traverser that will emit a prefix of the original traverser,
     * up to the item for which the predicate fails (exclusive).
     */
    @Nonnull
    default Traverser<T> takeWhile(@Nonnull Predicate<? super T> pred) {
        return new Traverser<T>() {
            boolean predicateSatisfied = true;

            @Override
            public T next() {
                if (!predicateSatisfied) {
                    return null;
                }
                T t = Traverser.this.next();
                predicateSatisfied = t == null || pred.test(t);
                return predicateSatisfied ? t : null;
            }
        };
    }

    /**
     * Returns a traverser that will emit a suffix of the original traverser,
     * starting from the item for which the predicate fails (inclusive).
     */
    @Nonnull
    default Traverser<T> dropWhile(@Nonnull Predicate<? super T> pred) {
        return new Traverser<T>() {
            boolean predicateSatisfied = true;

            @Override
            public T next() {
                if (!predicateSatisfied) {
                    return Traverser.this.next();
                }
                for (T t; (t = Traverser.this.next()) != null; ) {
                    predicateSatisfied = pred.test(t);
                    if (!predicateSatisfied) {
                        return t;
                    }
                }
                return null;
            }
        };
    }

    /**
     * Returns a traverser that will return all the items of this traverser,
     * plus an additional item once this one returns {@code null}. After that
     * it continues forwarding the return values of this traverser. It is
     * meant to be used on finite traversers.
     * <p>
     * Default implementations always returns a new traverser instance. If you
     * need to append multiple objects or use this method frequently,
     * {@link com.hazelcast.jet.core.AppendableTraverser} might be a better
     * choice.
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
     * Returns a traverser that will emit the same items as this traverser,
     * additionally passing each (non-null) item to the supplied consumer.
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
     * additionally run the supplied action the first time this traverser
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
