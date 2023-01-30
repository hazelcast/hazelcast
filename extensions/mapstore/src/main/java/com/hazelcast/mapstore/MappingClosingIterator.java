/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.mapstore;

import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterator providing mapping for the iterated objects from type T to R and
 * also implementing {@link Closeable}.
 *
 * @param <T> type of the items in the original iterator
 * @param <R> type of the mapped iterator
 */
class MappingClosingIterator<T, R> implements Iterator<R>, Closeable {

    private final Iterator<T> iterator;
    private final Function<T, R> mapFn;
    private final Runnable closeFn;

    /**
     * Creates the iterator
     *
     * @param iterator the original iterator that will be mapped
     * @param mapFn    the mapping function that will be applied to each element
     * @param closeFn  close runnable that the {@link #close()} delegates to
     */
    MappingClosingIterator(Iterator<T> iterator, Function<T, R> mapFn, Runnable closeFn) {
        this.iterator = iterator;
        this.mapFn = mapFn;
        this.closeFn = closeFn;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public R next() {
        return mapFn.apply(iterator.next());
    }

    @Override
    public void close() {
        closeFn.run();
    }
}
