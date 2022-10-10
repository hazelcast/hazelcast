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

import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class SlabAllocatorTest {

    @Test
    public void test() {
        SlabAllocator<LinkedList> slabAllocator = new SlabAllocator(10, LinkedList::new);

        LinkedList l1 = slabAllocator.allocate();
        LinkedList l2 = slabAllocator.allocate();

        assertNotNull(l1);
        assertNotNull(l2);
        assertNotSame(l1, l2);

        slabAllocator.free(l1);
        slabAllocator.free(l2);

        LinkedList l3 = slabAllocator.allocate();
        LinkedList l4 = slabAllocator.allocate();

        assertSame(l1, l4);
        assertSame(l2, l3);
    }

    @Test
    public void test_free_whenFull() {
        int capacity = 10;
        SlabAllocator<AtomicLong> slabAllocator = new SlabAllocator(capacity, AtomicLong::new);

        List<AtomicLong> objects = new ArrayList<>();
        // allocate a bunch of items
        for (int k = 0; k < capacity; k++) {
            objects.add(slabAllocator.allocate());
        }

        // free the items
        for (int k = 0; k < capacity; k++) {
            slabAllocator.free(objects.get(k));
        }

        // and free an additional item
        slabAllocator.free(new AtomicLong());
    }

    @Test(expected = NullPointerException.class)
    public void test_free_whenNull() {
        SlabAllocator<LinkedList> slabAllocator = new SlabAllocator(10, LinkedList::new);
        slabAllocator.free(null);
    }
}
