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

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;

public class ReadOnlyCollectionDelegate<T> extends AbstractSet<T> {

    @Nonnull
    private final Collection<T> delegate;

    public ReadOnlyCollectionDelegate(final Collection<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private final Iterator<? extends T> i = delegate.iterator();

            public boolean hasNext() {
                return i.hasNext();
            }

            public T next() {
                return i.next();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        return delegate.contains(o);
    }

}

