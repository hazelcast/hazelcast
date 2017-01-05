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

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Javadoc pending.
 */
public class Suppliers {

    /**
     * Returns a supplier of all the items retrieved from an iterator. Both the iterator
     * and each iterator's element are obtained lazily. The iterator is obtained from this
     * method's argument just once, upon the first invocation of {@code get()}. Each
     * invocation of {@code get()} consumes and returns a single item from the iterator.
     * After the iterator has been exhausted, {@code get()} keeps returning {@code null}.
     */
    public static <T> Supplier<T> lazyIterate(Supplier<Iterator<T>> iterSupplier) {
        return new LazyIteratingSupplier<>(iterSupplier);
    }

    /**
     * Wraps a {@code Supplier} and returns a memoizing supplier which calls
     * it only on the first invocation of {@code get()}, and afterwards
     * returns the remembered instance.
     */
    public static <T> Supplier<T> memoize(Supplier<T> onceSupplier) {
        return new MemoizingSupplier<>(onceSupplier);
    }

    /**
     * Adds a transformation layer on top of an existing supplier. The returned supplier
     * will provide the result of applying the given transformation to the value obtained
     * from the wrapped supplier.
     */
    public static <T, R> MappingSupplier<T, R> map(Supplier<T> wrapped, Function<T, R> transformation) {
        return new MappingSupplier<>(wrapped, transformation);
    }

    /**
     * Adds an action layer on top of an existing supplier. The returned supplier will
     * perform the given action on each item obtained from the wrapped supplier.
     */
    public static <T> MappingSupplier<T, T> peek(Supplier<T> wrapped, Consumer<T> action) {
        return new MappingSupplier<>(wrapped, item -> {
            action.accept(item);
            return item;
        });
    }

    static class LazyIteratingSupplier<T> implements Supplier<T> {
        private final Supplier<Iterator<T>> iterSupplier;
        private Iterator<T> iterator;

        private LazyIteratingSupplier(Supplier<Iterator<T>> iterSupplier) {
            this.iterSupplier = iterSupplier;
        }

        @Override
        public T get() {
            final Iterator<T> it = iterator != null ? iterator : (iterator = iterSupplier.get());
            return it.hasNext() ? it.next() : null;
        }
    }

    static class MappingSupplier<T, R> implements Supplier<R> {

        private final Supplier<T> wrapped;
        private final Function<T, R> transformation;

        private MappingSupplier(Supplier<T> wrapped, Function<T, R> transformation) {
            this.wrapped = wrapped;
            this.transformation = transformation;
        }

        @Override
        public R get() {
            final T item = wrapped.get();
            return item != null ? transformation.apply(item) : null;
        }
    }


    static final class MemoizingSupplier<T> implements Supplier<T> {
        private Supplier<T> onceSupplier;
        private T remembered;

        private MemoizingSupplier(Supplier<T> onceSupplier) {
            this.onceSupplier = onceSupplier;
        }

        @Override
        public T get() {
            if (onceSupplier == null) {
                return remembered;
            }
            remembered = onceSupplier.get();
            onceSupplier = null;
            return remembered;
        }
    }
}
