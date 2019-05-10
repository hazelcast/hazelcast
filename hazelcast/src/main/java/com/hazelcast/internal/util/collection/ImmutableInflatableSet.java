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

package com.hazelcast.internal.util.collection;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import com.hazelcast.nio.serialization.SerializableByConvention;

/**
 * An immutable {@link InflatableSet}.
 *
 * @param <T> the type of elements maintained by this set
 */
@SerializableByConvention
public final class ImmutableInflatableSet<T> extends InflatableSet<T> {

    private static final long serialVersionUID = 1L;

    private ImmutableInflatableSet(List<T> compactList) {
        super(compactList);
    }

    public static <T> ImmutableBuilder<T> newImmutableBuilder(int initialCapacity) {
        return new ImmutableBuilder<T>(initialCapacity);
    }

    @Override
    public Iterator<T> iterator() {
        if (state == State.INFLATED) {
            return Collections.unmodifiableSet(inflatedSet).iterator();
        }
        return new ImmutableHybridIterator();
    }

    private final class ImmutableHybridIterator extends HybridIterator {

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Builder for {@link ImmutableInflatableSet}.
     * This is the only way to create a new instance of ImmutableInflatableSet.
     *
     * @param <T> the type of elements maintained by this set
     */
    public static final class ImmutableBuilder<T> extends AbstractBuilder<T> {

        private ImmutableBuilder(int initialCapacity) {
            super(initialCapacity);
        }

        private ImmutableBuilder(List<T> list) {
            super(list);
        }

        public ImmutableBuilder<T> add(T item) {
            super.add(item);
            return this;
        }

        public ImmutableInflatableSet<T> build() {
            ImmutableInflatableSet<T> set = new ImmutableInflatableSet<T>(list);

            // make sure no further insertions are possible
            list = Collections.emptyList();
            return set;
        }
    }

    @Override
    public boolean add(T t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> coll) {
        throw new UnsupportedOperationException();
    }
}
