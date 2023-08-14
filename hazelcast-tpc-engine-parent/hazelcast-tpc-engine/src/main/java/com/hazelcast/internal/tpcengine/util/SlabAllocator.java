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
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * An allocator that contains an array with pooled object instances of the same
 * type.
 * <p/>
 * The SlabAllocator is eager with construction; so it will immediately fill the
 * slab with all the objects.
 * <p/>
 * Cleaning of the pooled objects before freeing is a responsibility of the caller.
 * <p/>
 * The allocator tries to keep the objects in the CPU cache by allocating and
 * freeing on the beginning of the slab instead of allocating on one side and
 * freeing on the other like a ringbuffer.
 * <p/>
 * The SlabAllocator creates the objects eagerly; so on construction.
 * <p/>
 * The SlabAllocator doesn't verify if the freed object originated from that
 * SlabAllocator.
 *
 * @param <E>
 */
public final class SlabAllocator<E> {

    private final int capacity;
    private final E[] array;
    // Points to the next object to get from the array
    private int index;

    /**
     * Creates a SlabAllocator.
     *
     * @param capacity      the capacity
     * @param constructorFn the function to create objects.
     * @throws IllegalArgumentException if capacity isn't positive.
     * @throws NullPointerException     if constructorFn is null or if an object
     *                                  returned from that constructorFn is null.
     */
    public SlabAllocator(int capacity, Supplier<E> constructorFn) {
        this.capacity = checkPositive(capacity, "capacity");
        checkNotNull(constructorFn, "constructorFn");

        this.array = (E[]) new Object[capacity];
        for (int k = 0; k < capacity; k++) {
            E object = constructorFn.get();
            if (object == null) {
                throw new NullPointerException("constructorFn can't return null");
            }
            array[k] = object;
        }
    }

    /**
     * Allocates a single object.
     *
     * @return the allocated object or <code>null</code> if there was no more
     * available object in the slab.
     */
    public E allocate() {
        if (index == capacity) {
            return null;
        }

        E object = array[index];
        // There is no need to null that element in the array. This saves a
        // write barrier.
        index++;
        return object;
    }

    /**
     * Frees the allocated object by returning it to the slab.
     *
     * @param object the object to free.
     * @throws NullPointerException  if object is null.
     * @throws IllegalStateException if more objects are freed than fit in this
     *                               SlabAllocator. If this happens there must
     *                               a coding problem because you free more often
     *                               than you allocate.
     */
    public void free(E object) {
        checkNotNull(object);

        if (index == 0) {
            throw new IllegalStateException();
        }

        index--;
        array[index] = object;
    }
}
