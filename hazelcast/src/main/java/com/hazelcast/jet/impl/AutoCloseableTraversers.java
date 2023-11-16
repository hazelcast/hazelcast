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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.impl.util.AutoCloseableTraverser;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class AutoCloseableTraversers {

    private static final AutoCloseableTraverser<Object> EMPTY_AUTOCLOSEABLE_TRAVERSER = new EmptyAutoCloseableTraverser<>();

    private AutoCloseableTraversers() {
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> AutoCloseableTraverser<T> emptyAutoCloseableTraverser() {
        return (AutoCloseableTraverser<T>) EMPTY_AUTOCLOSEABLE_TRAVERSER;
    }

    /**
     * Returns an adapter from {@code Iterator} to {@code Traverser}. The
     * iterator must return non-{@code null} items. Each time its {@code next()}
     * method is called, the traverser will take another item from the iterator
     * and return it.
     */
    @Nonnull
    public static <T> AutoCloseableTraverser<T> traverseAutoCloseableIterator(@Nonnull Iterator<? extends T> iterator) {
        return new AutoCloseableTraverser<>() {
            @Override
            public T next() {
                return iterator.hasNext() ? requireNonNull(iterator.next(), "Iterator returned a null item") : null;
            }

            @Override
            public void close() throws Exception {
                if (iterator instanceof AutoCloseable) {
                    AutoCloseable autoCloseable = (AutoCloseable) iterator;
                    autoCloseable.close();
                }
            }
        };
    }

    private static final class EmptyAutoCloseableTraverser<T> implements AutoCloseableTraverser<T> {
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
}
