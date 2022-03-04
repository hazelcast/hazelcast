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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.internal.networking.nio.SelectorOptimizer.IteratorImpl;
import com.hazelcast.internal.networking.nio.SelectorOptimizer.SelectionKeys;
import com.hazelcast.internal.networking.nio.SelectorOptimizer.SelectionKeysSet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SelectionKeysSetTest extends HazelcastTestSupport {

    private final SelectionKey key1 = mock(SelectionKey.class);
    private final SelectionKey key2 = mock(SelectionKey.class);
    private final SelectionKey key3 = mock(SelectionKey.class);
    private SelectionKeysSet selectionKeysSet;

    @Before
    public void setup() {
        selectionKeysSet = new SelectionKeysSet();
    }

    @Test
    public void remove_doesNothing() {
        assertFalse(selectionKeysSet.remove(key1));
    }

    @Test
    public void contains_doesNothing() {
        assertFalse(selectionKeysSet.contains(key1));
    }

    @Test
    public void iteratorRecycled() {
        Iterator it1 = selectionKeysSet.iterator();
        Iterator it2 = selectionKeysSet.iterator();
        assertSame(it1, it2);
    }

    @Test
    public void add_whenCapacityNotSufficient() {
        List<SelectionKey> expectedKeys = new LinkedList<SelectionKey>();
        for (int k = 0; k < SelectionKeys.INITIAL_CAPACITY * 4; k++) {
            SelectionKey key = mock(SelectionKey.class);
            expectedKeys.add(key);

            selectionKeysSet.add(key);
            assertEquals(expectedKeys.size(), selectionKeysSet.size());
        }

        SelectionKeys active = selectionKeysSet.activeKeys;
        assertEquals(active.size, expectedKeys.size());
        for (int k = 0; k < expectedKeys.size(); k++) {
            SelectionKey expected = expectedKeys.get(k);
            SelectionKey found = active.keys[k];
            assertSame(expected, found);
        }
    }

    @Test
    public void add_whenNull() {
        boolean result = selectionKeysSet.add(null);

        assertFalse(result);
        SelectionKeys active = selectionKeysSet.activeKeys;
        assertEquals(0, active.size);
    }

    // tests the regular nio loop; hasNext, next, remove.
    @Test
    public void testLoop() {
        List<SelectionKey> addedKeys = Arrays.asList(key1, key2, key3);

        for (SelectionKey selectionKey : addedKeys) {
            selectionKeysSet.add(selectionKey);
        }

        IteratorImpl it = (IteratorImpl) selectionKeysSet.iterator();
        int k = 0;
        for (SelectionKey expected : addedKeys) {
            // check if the hasNext returns true
            boolean hasNext = it.hasNext();
            assertTrue(hasNext);

            // check if the key is correct.
            SelectionKey next = it.next();
            assertSame(expected, next);
            assertEquals(k, it.index);

            // do the remove and check if the slot is nulled
            it.remove();
            assertNull(it.keys[it.index]);
            k++;
        }

        assertFalse(it.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void next_whenNoItem() {
        IteratorImpl it = (IteratorImpl) selectionKeysSet.iterator();

        it.next();
    }

    @Test(expected = IllegalStateException.class)
    public void remove_whenNoItem() {
        IteratorImpl it = (IteratorImpl) selectionKeysSet.iterator();

        it.remove();
    }

    // see https://github.com/hazelcast/hazelcast/issues/10436
    @Test
    public void remove_whenLastItemFromArray() {
        for (int k = 0; k < SelectionKeys.INITIAL_CAPACITY; k++) {
            selectionKeysSet.add(mock(SelectionKey.class));
        }

        IteratorImpl it = (IteratorImpl) selectionKeysSet.iterator();
        // we now next/remove all items apart from the last one.
        for (int k = 0; k < SelectionKeys.INITIAL_CAPACITY - 1; k++) {
            it.next();
            it.remove();
        }

        // last item we next
        it.next();
        // and we remove; with the bug we would get a IllegalStateException
        it.remove();
        assertFalse(it.hasNext());
    }
}
