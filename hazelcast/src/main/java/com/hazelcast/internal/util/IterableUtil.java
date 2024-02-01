/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Utility functions for working with {@link Iterable}
 */
public final class IterableUtil {

    private IterableUtil() {
    }

    /**
     * @return First element or defaultValue if iterable is empty
     */
    public static <T> T getFirst(Iterable<T> iterable, T defaultValue) {
        Iterator<T> iterator = iterable.iterator();
        return iterator.hasNext() ? iterator.next() : defaultValue;
    }

    /**
     * Transform the Iterable by applying a function to each element
     **/
    public static <T, R> Iterable<R> map(Iterable<T> iterable, Function<T, R> mapper) {
        return () -> map(iterable.iterator(), mapper);
    }

    /**
     * Transform the Iterator by applying a function to each element
     **/
    public static <T, R> Iterator<R> map(Iterator<T> iterator, Function<T, R> mapper) {
        return new Iterator<R>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public R next() {
                return mapper.apply(iterator.next());
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    /**
     * Lazily filters iterated elements.
     *
     * @return a new iterable object which
     * has an iterator capable of filtering elements
     */
    public static <T> Iterable<T> filter(Iterable<T> iterable, Predicate<T> filter) {
        Iterator<T> givenIterator = iterable.iterator();

        @SuppressWarnings("checkstyle:anoninnerlength")
        Iterator<T> filteringIterator = new Iterator<T>() {
            private T next;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                }

                while (givenIterator.hasNext()) {
                    T temp = givenIterator.next();
                    if (filter.test(temp)) {
                        next = temp;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                T nextLocal = next;
                next = null;
                return nextLocal;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return () -> filteringIterator;
    }

    /**
     * @return size of iterable
     */
    public static int size(Iterable iterable) {
        checkNotNull(iterable, "iterable cannot be null");

        int size = 0;
        Iterator iterator = iterable.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            size++;
        }
        return size;
    }

    public static <R> Iterator<R> limit(final Iterator<R> iterator, final int limit) {
        return new Iterator<R>() {
            private int iterated;

            @Override
            public boolean hasNext() {
                return iterated < limit && iterator.hasNext();
            }

            @Override
            public R next() {
                iterated++;
                return iterator.next();
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    /**
     * Return empty Iterable if argument is null
     **/
    public static <T> Iterable<T> nullToEmpty(Iterable<T> iterable) {
        return iterable == null ? Collections.emptyList() : iterable;
    }

    /**
     * @return a read only iterator.
     * @throws UnsupportedOperationException
     */
    public static <T> Iterator<T> asReadOnlyIterator(Iterator<T> iterator) {
        if (iterator instanceof UnmodifiableIterator) {
            return iterator;
        }

        return new UnmodifiableIterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }
        };
    }

    /**
     * Add an element to the beginning of an iterator.
     *
     * @param prepend element to add.
     * @param iterator the iterator to which an element will be added. A null value is not supported.
     * @return iterator with prepended element
     */
    public static <T> Iterator<T> prepend(T prepend, @Nonnull Iterator<? extends T> iterator) {
        checkNotNull(iterator, "iterator cannot be null.");
        return new PrependIterator<>(prepend, iterator);
    }

    /**
     * Skip elements until the first element satisfying the predicate is encountered.
     *
     * @param iterator the iterator whose first elements should be skipped. A null value is not supported.
     * @param predicate a condition indicating when to stop skipping elements.
     * @return iterator
     */
    public static <T> Iterator<T> skipFirst(Iterator<T> iterator, @Nonnull Predicate<? super T> predicate) {
        checkNotNull(iterator, "iterator cannot be null.");
        while (iterator.hasNext()) {
            T object = iterator.next();
            if (!predicate.test(object)) {
                continue;
            }
            return prepend(object, iterator);
        }
        return iterator;
    }

    private static class PrependIterator<E> implements Iterator<E> {
        E prependElement;
        Iterator<? extends E> iterator;

        PrependIterator(E prependElement, Iterator<? extends E> iterator) {
            this.prependElement = prependElement;
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return prependElement != null || iterator.hasNext();
        }

        @Override
        public E next() {
            if (prependElement != null) {
                var value = prependElement;
                prependElement = null;
                return value;
            }
            return iterator.next();
        }
    }
}
