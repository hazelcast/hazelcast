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

package com.hazelcast.jet;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ResettableSingletonTraverser;

import javax.annotation.Nonnull;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Utility class with several {@link Traverser}s useful in {@link Processor}
 * implementations.
 *
 * @since Jet 3.0
 */
public final class Traversers {

    private static final Traverser<Object> EMPTY_TRAVERSER = new EmptyTraverser<>();

    private Traversers() {
    }

    /**
     * Returns a traverser that always returns {@code null}.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> Traverser<T> empty() {
        return (Traverser<T>) EMPTY_TRAVERSER;
    }

    /**
     * Returns a traverser over the given single item.
     * <p>
     * You can use {@link ResettableSingletonTraverser} for less GC litter,
     * if you can reuse the traverser instance.
     */
    @Nonnull
    public static <T> Traverser<T> singleton(@Nonnull T item) {
        return new SingletonTraverser<>(item);
    }

    /**
     * Returns an adapter from {@code Iterator} to {@code Traverser}. The
     * iterator must return non-{@code null} items. Each time its {@code next()}
     * method is called, the traverser will take another item from the iterator
     * and return it.
     */
    @Nonnull
    public static <T> Traverser<T> traverseIterator(@Nonnull Iterator<? extends T> iterator) {
        return () -> iterator.hasNext() ? requireNonNull(iterator.next(), "Iterator returned a null item") : null;
    }

    /**
     * Returns an adapter from {@code Iterator} to {@code Traverser}. Each time
     * its {@code next()} method is called, the traverser will take another
     * item from the iterator and return it.
     *
     * @param ignoreNulls if {@code true}, null elements form the iterator will be
     *                    filtered out. If {@code false}, error will be thrown on null elements.
     */
    @Nonnull
    public static <T> Traverser<T> traverseIterator(@Nonnull Iterator<? extends T> iterator, boolean ignoreNulls) {
        if (!ignoreNulls) {
            return traverseIterator(iterator);
        }
        return () -> {
            while (iterator.hasNext()) {
                T next = iterator.next();
                if (next != null) {
                    return next;
                }
            }
            return null;
        };
    }

    /**
     * Returns an adapter from {@code Spliterator} to {@code Traverser}. Each
     * time its {@code next()} method is called, the traverser calls {@link
     * Spliterator#tryAdvance(Consumer)}. If it returns {@code true}, the
     * traverser returns the item it emitted to the consumer; otherwise the
     * traverser returns {@code null}. The spliterator must not emit {@code
     * null} items.
     */
    @Nonnull
    public static <T> Traverser<T> traverseSpliterator(@Nonnull Spliterator<T> spliterator) {
        return new SpliteratorTraverser<>(spliterator);
    }

    /**
     * Returns an adapter from {@code Enumeration} to {@code Traverser}. Each
     * time its {@code next()} method is called, the traverser takes another
     * item from the enumeration and returns it. The enumeration must not
     * contain {@code null} items.
     */
    @Nonnull
    public static <T> Traverser<T> traverseEnumeration(@Nonnull Enumeration<T> enumeration) {
        return () -> enumeration.hasMoreElements()
                ? requireNonNull(enumeration.nextElement(), "Enumeration contains a null element")
                : null;
    }

    /**
     * Returns a traverser over the given stream. It will traverse it through
     * its spliterator, which it obtains immediately. When it exhausts the
     * stream, it closes it. The stream must not contain {@code null} items.
     */
    @Nonnull
    public static <T> Traverser<T> traverseStream(@Nonnull Stream<T> stream) {
        return traverseSpliterator(stream.spliterator()).onFirstNull(stream::close);
    }

    /**
     * Returns a traverser over the given iterable. It obtains the iterator
     * immediately.
     */
    @Nonnull
    public static <T> Traverser<T> traverseIterable(@Nonnull Iterable<? extends T> iterable) {
        return traverseIterator(iterable.iterator());
    }

    /**
     * Returns a traverser over the given array. Null elements in the array are
     * skipped.
     */
    @Nonnull
    public static <T> Traverser<T> traverseArray(@Nonnull T[] array) {
        return new ArrayTraverser<>(array);
    }

    /**
     * Returns a traverser over the supplied arguments (or item array). Null
     * items are skipped.
     *
     * @param items the items to traverse over
     * @param <T> type of the items
     */
    @SafeVarargs
    public static <T> Traverser<T> traverseItems(T... items) {
        return traverseArray(items);
    }

    /**
     * Flattens a supplier of traverser into a lazy-initialized traverser. It
     * obtains the traverser from this method's argument just once, upon the
     * first invocation of {@code get()}.
     */
    @Nonnull
    public static <T> Traverser<T> lazy(@Nonnull Supplier<Traverser<T>> supplierOfTraverser) {
        return new LazyTraverser<>(supplierOfTraverser);
    }

    private static final class LazyTraverser<T> implements Traverser<T> {
        private Supplier<Traverser<T>> supplierOfTraverser;
        private Traverser<T> traverser;

        LazyTraverser(@Nonnull Supplier<Traverser<T>> supplierOfTraverser) {
            this.supplierOfTraverser = supplierOfTraverser;
        }

        @Override
        public T next() {
            final Traverser<T> trav = this.traverser;
            if (trav != null) {
                return trav.next();
            }
            try {
                traverser = supplierOfTraverser.get();
                return traverser.next();
            } finally {
                supplierOfTraverser = null;
            }
        }
    }

    private static class ArrayTraverser<T> implements Traverser<T> {
        private int index;
        private final T[] array;

        ArrayTraverser(@Nonnull T[] array) {
            this.array = array;
        }

        @Override
        public T next() {
            while (index < array.length) {
                try {
                    T t = array[index];
                    if (t != null) {
                        return t;
                    }
                } finally {
                    index++;
                }
            }
            return null;
        }
    }

    private static class SpliteratorTraverser<T> implements Traverser<T>, Consumer<T> {
        private final Spliterator<T> spliterator;
        private T nextItem;

        SpliteratorTraverser(Spliterator<T> spliterator) {
            this.spliterator = spliterator;
        }

        @Override
        public T next() {
            try {
                boolean advanced = spliterator.tryAdvance(this);
                if (advanced) {
                    requireNonNull(nextItem);
                }
                return nextItem;
            } finally {
                nextItem = null;
            }
        }

        @Override
        public void accept(T t) {
            nextItem = t;
        }
    }

    private static final class EmptyTraverser<T> implements Traverser<T> {
        @Override
        public T next() {
            return null;
        }

        @SuppressWarnings("unchecked")
        @Nonnull @Override
        public <R> Traverser<R> map(@Nonnull Function<? super T, ? extends R> mapFn) {
            return (Traverser<R>) this;
        }

        @SuppressWarnings("unchecked")
        @Nonnull @Override
        public <R> Traverser<R> flatMap(@Nonnull Function<? super T, ? extends Traverser<R>> flatMapFn) {
            return (Traverser<R>) this;
        }

        @Nonnull @Override
        public Traverser<T> filter(@Nonnull Predicate<? super T> filterFn) {
            return this;
        }

        @Nonnull @Override
        public Traverser<T> takeWhile(@Nonnull Predicate<? super T> predicate) {
            return this;
        }

        @Nonnull @Override
        public Traverser<T> dropWhile(@Nonnull Predicate<? super T> predicate) {
            return this;
        }

        @Nonnull @Override
        public Traverser<T> peek(@Nonnull Consumer<? super T> action) {
            return this;
        }
    }

    private static final class SingletonTraverser<T> implements Traverser<T> {
        private T item;

        SingletonTraverser(@Nonnull T item) {
            this.item = item;
        }

        @Override
        public T next() {
            try {
                return item;
            } finally {
                item = null;
            }
        }

        @Nonnull @Override
        // an optimized version to map in-place
        public <R> Traverser<R> map(@Nonnull Function<? super T, ? extends R> mapFn) {
            @SuppressWarnings("unchecked")
            SingletonTraverser<R> newThis = (SingletonTraverser<R>) this;
            if (item != null) {
                newThis.item = mapFn.apply(item);
            }
            return newThis;
        }
    }
}
