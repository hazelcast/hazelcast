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

public class ReadOnlyMultiCollectionDelegate<T> extends AbstractSet<T> {

    @Nonnull
    private final Collection<Collection<T>> delegates;

    private final int size;

    public ReadOnlyMultiCollectionDelegate(final Collection<Collection<T>> delegates, int size) {
        this.delegates = delegates;
        this.size = size;
    }

    @Override
    public Iterator<T> iterator() {
        return new ReadOnlyMultiCollectionIterator(delegates.iterator());
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(final Object o) {
        throw new UnsupportedOperationException();
    }

}

