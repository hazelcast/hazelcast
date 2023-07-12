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

package com.hazelcast.internal.tpcengine.util;

import java.util.function.Supplier;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;

/**
 * An allocator that contains an array with pooled object instances of the same type.
 * <p>
 * LIFO would be more cache friendly
 *
 * @param <E>
 */
public final class SlabAllocator<E> {

    private final Supplier<E> constructorFn;
    private E[] array;
    private int index = -1;

    public SlabAllocator(int capacity, Supplier<E> constructorFn) {
        this.array = (E[]) new Object[capacity];
        this.constructorFn = checkNotNull(constructorFn);
    }

    // todo: an allocator should be able to return null. Currently the capacity isn't respected.
    public E allocate() {
        if (index == -1) {
            return constructorFn.get();
        }

        E object = array[index];
        array[index] = null;
        index--;
        return object;
    }

    public void free(E e) {
        checkNotNull(e);

        if (index < array.length - 1) {
            index++;
            array[index] = e;
        }
    }
}
