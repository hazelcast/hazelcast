/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Arrays;

/**
 * A very fast implementation of {@link java.util.List} that stores its content in a non-resizeable array.
 * The {@link FixedCapacityArrayList} provides an API to fetch that array. Fetching that array makes
 * the list instance unusable.
 */
@NotThreadSafe
public class FixedCapacityArrayList<E> extends AbstractList<E> {
    private E[] elementData;
    private int size;

    @SuppressWarnings("unchecked")
    public FixedCapacityArrayList(Class<E> eClass, int initialCapacity) {
        this.elementData = (E[]) Array.newInstance(eClass, initialCapacity);
    }

    @Override
    public boolean add(E e) {
        modCount++;
        elementData[size++] = e;
        return true;
    }

    @Override
    public E get(int index) {
        return elementData[index];
    }

    @Override
    public int size() {
        return size;
    }

    /**
     * Returns a storage with all the elements and makes this instance unusable.
     */
    public E[] asArray() {
        E[] result = size == elementData.length ? elementData : Arrays.copyOfRange(elementData, 0, size);
        elementData = null;
        return result;
    }
}
