/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Read-only collection which delegates its operations to an underlying map and a couple of functions. Designed
 * to easily implement keyset() and values() methods in a map.
 *
 * @param <V> The generic type of the set.
 */
public final class MapDelegatingSet<V> extends AbstractSet<V> {
    private final Map<?, ?> delegate;
    private final Supplier<Iterator<V>> iterator;
    private final Predicate contains;

    public MapDelegatingSet(final Map<?, ?> delegate, final Supplier<Iterator<V>> iterator, final Predicate contains) {
        this.delegate = delegate;
        this.iterator = iterator;
        this.contains = contains;
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        return delegate.size();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public boolean contains(final Object o) {
        return contains.test(o);
    }

    /**
     * {@inheritDoc}
     */
    public Iterator<V> iterator() {
        return iterator.get();
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        delegate.clear();
    }
}
