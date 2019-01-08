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

package com.hazelcast.query.impl.collections;

import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReadOnlyFilterableCollectionDelegate<T> extends AbstractSet<T> {

    private static final int SIZE_UNINITIALIZED = -1;

    @Nonnull
    private final Set<T> delegate;

    @Nonnull
    private final List<Predicate> filters;

    private int size;

    public ReadOnlyFilterableCollectionDelegate(@Nonnull Set<T> delegate,
                                                @Nonnull List<Predicate> filters) {
        this.delegate = delegate;
        this.filters = filters;
        size = SIZE_UNINITIALIZED;
    }

    @Override
    public Iterator<T> iterator() {
        return new ReadOnlyFilterIterator(delegate.iterator(), filters);
    }

    @Override
    public int size() {
        if (size == SIZE_UNINITIALIZED) {
            int calculatedSize = 0;
            for (Iterator<T> it = iterator(); it.hasNext(); it.next()) {
                calculatedSize++;
            }
            size = calculatedSize;
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        throw new UnsupportedOperationException();
    }

}

