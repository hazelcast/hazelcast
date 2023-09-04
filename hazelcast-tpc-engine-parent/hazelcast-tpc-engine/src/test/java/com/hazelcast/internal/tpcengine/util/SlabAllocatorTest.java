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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class SlabAllocatorTest {

    @Test
    public void test_construct_whenNegativeCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new SlabAllocator<>(-1, () -> "foo"));
    }

    @Test
    public void test_construct_whenZeroCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new SlabAllocator<>(0, () -> "foo"));
    }

    @Test
    public void test_construct_whenNullConstructorFn() {
        assertThrows(NullPointerException.class, () -> new SlabAllocator<>(10, null));
    }

    @Test
    public void test_construct_whenConstructorFnReturnsNull() {
        assertThrows(NullPointerException.class, () -> new SlabAllocator<>(10, () -> null));
    }

    @Test
    public void test_allocate() {
        int capacity = 10;
        SlabAllocator<Integer> slabAllocator = new SlabAllocator<>(10, new IntegerSupplier());

        for (int k = 0; k < capacity; k++) {
            assertEquals(Integer.valueOf(k), slabAllocator.allocate());
        }

        assertNull(slabAllocator.allocate());
    }

    @Test
    public void test() {
        SlabAllocator<Integer> slabAllocator = new SlabAllocator<>(10, new IntegerSupplier());

        Integer a1 = slabAllocator.allocate();
        assertEquals(Integer.valueOf(0), a1);

        Integer a2 = slabAllocator.allocate();
        assertEquals(Integer.valueOf(1), a2);

        Integer a3 = slabAllocator.allocate();
        assertEquals(Integer.valueOf(2), a3);

        Integer a4 = slabAllocator.allocate();
        assertEquals(Integer.valueOf(3), a4);

        slabAllocator.free(a1);

        Integer a5 = slabAllocator.allocate();
        assertEquals(a1, a5);

        slabAllocator.free(a4);

        Integer a6 = slabAllocator.allocate();
        assertEquals(a4, a6);

        slabAllocator.free(a3);
        slabAllocator.free(a2);

        Integer a7 = slabAllocator.allocate();
        assertEquals(a2, a7);

        Integer a8 = slabAllocator.allocate();
        assertEquals(a3, a8);
    }

    @Test
    public void test_free_whenFull() {
        int capacity = 10;
        SlabAllocator<Integer> slabAllocator = new SlabAllocator<>(10, new IntegerSupplier());

        List<Integer> objects = new ArrayList<>();
        // allocate a bunch of items
        for (int k = 0; k < capacity; k++) {
            objects.add(slabAllocator.allocate());
        }

        // free the items
        for (int k = 0; k < capacity; k++) {
            slabAllocator.free(objects.get(k));
        }

        // and free an additional item
        assertThrows(IllegalStateException.class, () -> slabAllocator.free(Integer.valueOf(10)));
    }

    @Test
    public void test_free_whenNull_then() {
        SlabAllocator<Integer> slabAllocator = new SlabAllocator<>(10, new IntegerSupplier());
        assertThrows(NullPointerException.class, () -> slabAllocator.free(null));
    }

    private static class IntegerSupplier implements Supplier<Integer> {
        private int x;

        @Override
        public Integer get() {
            return x++;
        }
    }
}
