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

package com.hazelcast.internal.tpc.util;

import java.util.function.Supplier;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;

/**
 * An allocator that contains an array with pooled object instances of the same type.
 *
 * @param <E>
 */
public final class SlabAllocator<E> {

    private final Supplier<E> supplier;
    private E[] array;
    private int index = -1;

    public SlabAllocator(int capacity, Supplier<E> supplier) {
        this.array = (E[]) new Object[capacity];
        this.supplier = checkNotNull(supplier);
    }

    public E allocate() {
        if (index == -1) {
            return supplier.get();
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
