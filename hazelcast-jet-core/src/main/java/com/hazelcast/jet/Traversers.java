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

import javax.annotation.Nonnull;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Utility class with several {@link Traverser}s useful in {@link Processor}
 * implementations.
 */
public final class Traversers {

    private Traversers() {
    }

    /**
     * Returns a traverser that always returns null.
     */
    @Nonnull
    public static <T> Traverser<T> empty() {
        return () -> null;
    }

    /**
     * Returns a simple adapter from {@code Iterator} to {@code Traverser}. The
     * iterator must return non-{@code null} items. Each time its {@code next()}
     * method is called, the traverser will take another item from the iterator
     * and return it.
     */
    @Nonnull
    public static <T> Traverser<T> iterate(@Nonnull Iterator<T> iterator) {
        return () -> iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Returns a simple adapter from {@code Spliterator} to {@code Traverser}.
     * Each time its {@code next()} method is called, the traverser will take
     * another item from the spliterator and return it.
     */
    @Nonnull
    public static <T> Traverser<T> spliterate(@Nonnull Spliterator<T> spliterator) {
        final ResettableSingletonTraverser<T> trav = new ResettableSingletonTraverser<>();
        return () -> {
            spliterator.tryAdvance(trav);
            return trav.next();
        };
    }

    /**
     * Returns a simple adapter from {@code Enumeration} to {@code Traverser}. The
     * enumeration must return non-{@code null} items. Each time its {@code next()}
     * method is called, the traverser will take another item from the enumeration
     * and return it.
     */
    @Nonnull
    public static <T> Traverser<T> enumerate(@Nonnull Enumeration<T> enumeration) {
        return () -> enumeration.hasMoreElements() ? enumeration.nextElement() : null;
    }

    /**
     * Returns a traverser over the given stream. The stream is traversed through
     * its spliterator, which is obtained immediately.
     */
    @Nonnull
    public static <T> Traverser<T> traverseStream(@Nonnull Stream<T> stream) {
        return spliterate(stream.spliterator());
    }

    /**
     * Returns a traverser over the given iterable. The iterator is obtained
     * immediately.
     */
    @Nonnull
    public static <T> Traverser<T> traverseIterable(@Nonnull Iterable<T> iterable) {
        return iterate(iterable.iterator());
    }

    /**
     * Returns a traverser over the given array.
     */
    @Nonnull
    public static <T> Traverser<T> traverseArray(@Nonnull T[] array) {
        return new ArrayTraverser<>(array);
    }

    /**
     * Flattens a supplier of traverser into a lazy-initialized traverser. The
     * traverser is obtained from this method's argument just once, upon the
     * first invocation of {@code get()}.
     */
    @Nonnull
    public static <T> Traverser<T> lazy(@Nonnull Supplier<Traverser<T>> supplierOfTraverser) {
        return new LazyTraverser<>(supplierOfTraverser);
    }

    /**
     * Traverses over a single item which can be set from the outside, by using
     * this traverser as a {@code Consumer<T>}. Another item can be set at any
     * time and the subsequent {@code next()} call will consume it.
     *
     * @param <T> item type
     */
    public static class ResettableSingletonTraverser<T> implements Traverser<T>, Consumer<T> {
        T item;

        @Override
        public T next() {
            try {
                return item;
            } finally {
                item = null;
            }
        }

        /**
         * Resets this traverser so that the following {@code next()} call
         * will return the item supplied here.
         *
         * @param item the item to return from {@code next()}
         */
        @Override
        public void accept(T item) {
            this.item = item;
        }
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
                return (traverser = supplierOfTraverser.get()).next();
            } finally {
                supplierOfTraverser = null;
            }
        }
    }

    private static class ArrayTraverser<T> implements Traverser<T> {
        private int i;
        private final T[] array;

        ArrayTraverser(@Nonnull T[] array) {
            this.array = array;
        }

        @Override
        public T next() {
            return i < array.length && i >= 0 ? array[i++] : null;
        }
    }
}
