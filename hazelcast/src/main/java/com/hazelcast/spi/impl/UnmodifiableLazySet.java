/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * This is an unmodifiable lazy set which is not parameterized.
 *
 * <p>
 * If the underlying list contain {@link Data} elements, they will be deserialized and replaced with deserialized
 * form on access (thus the "Lazy" in the name).
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class UnmodifiableLazySet extends AbstractSet<Object> {
    private final UnmodifiableLazyList list;

    public UnmodifiableLazySet(List dataList, SerializationService serializationService) {
        list = new UnmodifiableLazyList(dataList, serializationService);
    }

    public static <T> Set<T> toImmutableLazySet(List<T> resultSet, SerializationService serializationService) {
        return (Set<T>) new UnmodifiableLazySet(resultSet, serializationService);
    }

    @Override
    @Nonnull
    public Iterator<Object> iterator() {
        return list.iterator();
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean addAll(Collection c) {
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
    public boolean removeIf(@Nonnull Predicate filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> coll) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
