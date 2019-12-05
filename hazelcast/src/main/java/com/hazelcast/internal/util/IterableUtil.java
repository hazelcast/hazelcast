/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

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

    /** Transform the Iterable by applying a function to each element  **/
    public static <T, R> Iterable<R> map(Iterable<T> iterable, Function<T, R> mapper) {
        return () -> map(iterable.iterator(), mapper);
    }

    /** Transform the Iterator by applying a function to each element  **/
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

    public static <T, R> Iterator<R> limit(final Iterator<R> iterator, final int limit) {
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

    /** Return empty Iterable if argument is null **/
    public static <T> Iterable<T> nullToEmpty(Iterable<T> iterable) {
        return iterable == null ? Collections.<T>emptyList() : iterable;
    }


}
