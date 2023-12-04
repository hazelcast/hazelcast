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

package com.hazelcast.internal.util.collection;

import com.hazelcast.internal.serialization.SerializableByConvention;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.IntFunction;

/**
 * Prevents any "sane" traversal, such as classical for loop with indices,
 * enhanced for loop with implicit iterator, usage of explicit iterators,
 * cloning or copy constructors. It can be used by in-place builders with
 * inherent locking mechanism, in which locking can be done via {@link
 * #toReadonlyList()}. Such builders are required to support cyclic
 * structures, such as {@code QueryDataType} and {@code HazelcastObjectType}.
 */
@SerializableByConvention
public class NonTraversableList<T> extends ArrayList<T> {
    private final String message;

    public NonTraversableList(String message) {
        this.message = message;
    }

    @SuppressWarnings("unchecked")
    public List<T> toReadonlyList() {
        return (List<T>) List.of(super.toArray());
    }

    private RuntimeException ise() {
        return new IllegalStateException(message);
    }

    @Override
    public int size() {
        throw ise();
    }

    @Nonnull @Override
    public Iterator<T> iterator() {
        throw ise();
    }

    @Nonnull @Override
    public ListIterator<T> listIterator() {
        throw ise();
    }

    @Nonnull @Override
    public ListIterator<T> listIterator(int index) {
        throw ise();
    }

    @Nonnull @Override
    public Object[] toArray() {
        throw ise();
    }

    @Override
    public <T1> T1[] toArray(IntFunction<T1[]> generator) {
        throw ise();
    }

    @Override
    @SuppressWarnings("SuperClone")
    public Object clone() {
        throw ise();
    }
}
