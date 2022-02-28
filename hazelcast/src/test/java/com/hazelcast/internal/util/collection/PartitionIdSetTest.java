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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ConstantConditions")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionIdSetTest {

    private PartitionIdSet partitionIdSet;

    @Before
    public void setup() {
        partitionIdSet = new PartitionIdSet(271);
    }

    @Test
    public void test_add() {
        partitionIdSet.add(3);
        assertTrue(partitionIdSet.contains(3));
        assertFalse(partitionIdSet.contains(2));

        partitionIdSet.add(Integer.valueOf(126));
        assertTrue(partitionIdSet.contains(126));
    }

    @Test
    public void test_addAll() {
        partitionIdSet.addAll(listOf(0, 1, 2, 3, 4));
        assertContents(partitionIdSet);
    }

    @Test
    public void test_addAll_fromPartitionIdSet() {
        partitionIdSet.addAll(listOf(0, 1, 2, 3, 4));
        PartitionIdSet other = new PartitionIdSet(271);
        other.addAll(partitionIdSet);
        assertContents(other);
    }

    @Test
    public void test_size() {
        assertEquals(0, partitionIdSet.size());
        partitionIdSet.add(3);
        assertEquals(1, partitionIdSet.size());
        partitionIdSet.add(7);
        assertEquals(2, partitionIdSet.size());
        partitionIdSet.add(3);
        assertEquals(2, partitionIdSet.size());

        partitionIdSet.remove(7);
        assertEquals(1, partitionIdSet.size());
        partitionIdSet.remove(7);
        assertEquals(1, partitionIdSet.size());
        partitionIdSet.remove(3);
        assertEquals(0, partitionIdSet.size());

        partitionIdSet.addAll(create(1, 2, 3, 4));
        assertEquals(4, partitionIdSet.size());

        partitionIdSet.removeAll(create(3, 4));
        assertEquals(2, partitionIdSet.size());

        partitionIdSet.clear();
        assertEquals(0, partitionIdSet.size());

        partitionIdSet.union(create(1, 2));
        assertEquals(2, partitionIdSet.size());
        partitionIdSet.union(create(2, 3));
        assertEquals(3, partitionIdSet.size());

        partitionIdSet.complement();
        assertEquals(268, partitionIdSet.size());
    }

    @Test
    public void test_isEmpty() {
        assertTrue(partitionIdSet.isEmpty());
        partitionIdSet.add(17);
        assertFalse(partitionIdSet.isEmpty());
        partitionIdSet.remove(17);
        assertTrue(partitionIdSet.isEmpty());
    }

    @Test
    public void test_remove() {
        partitionIdSet.add(19);

        assertTrue(partitionIdSet.remove(19));
        assertFalse(partitionIdSet.contains(19));

        assertFalse(partitionIdSet.remove(19));
    }

    @Test
    public void test_removeInteger() {
        partitionIdSet.add(19);
        Integer nineteen = 19;

        assertTrue(partitionIdSet.remove(nineteen));
        assertEquals(0, partitionIdSet.size());
    }

    @Test
    public void test_getPartitionCount() {
        assertEquals(271, partitionIdSet.getPartitionCount());
    }

    @Test
    public void test_clear() {
        partitionIdSet.addAll(listOf(1, 3, 5, 7, 9));
        partitionIdSet.clear();
        assertEquals(0, partitionIdSet.size());
        assertFalse(partitionIdSet.contains(1));
    }

    @Test
    public void test_iterator() {
        List<Integer> values = listOf(1, 3, 5, 7, 9);
        partitionIdSet.addAll(values);
        Iterator<Integer> iterator = partitionIdSet.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            assertEquals(values.get(i), iterator.next());
            i++;
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void test_iteratorThrowsException_whenNoNextElement() {
        partitionIdSet.iterator().next();
    }

    @Test
    public void test_intIterator() {
        List<Integer> values = listOf(1, 3, 5, 7, 9);
        partitionIdSet.addAll(values);
        PrimitiveIterator.OfInt intIterator = partitionIdSet.intIterator();
        int i = 0;
        while (intIterator.hasNext()) {
            assertEquals(values.get(i).intValue(), intIterator.nextInt());
            i++;
        }
    }

    @Test
    public void test_iteratorRemove() {
        partitionIdSet.addAll(listOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        PrimitiveIterator.OfInt iterator = partitionIdSet.intIterator();
        while (iterator.hasNext()) {
            int partitionId = iterator.nextInt();
            if (partitionId >= 5) {
                iterator.remove();
            }
        }
        assertContents(partitionIdSet);
    }

    @Test(expected = ClassCastException.class)
    public void test_contains_whenNotInteger() {
        partitionIdSet.contains(new Object());
    }

    @Test
    public void test_contains() {
        partitionIdSet.add(5);
        assertTrue(partitionIdSet.contains(Integer.valueOf(5)));
        assertTrue(partitionIdSet.contains(5));
    }

    @Test(expected = ClassCastException.class)
    public void test_remove_whenNotInteger() {
        partitionIdSet.remove(new Object());
    }

    @Test
    public void test_constructionWithInitialPartitionIds() {
        partitionIdSet = new PartitionIdSet(11, listOf(0, 1, 2, 3, 4));
        assertContents(partitionIdSet);
    }

    @Test
    public void test_copyConstructor() {
        partitionIdSet = new PartitionIdSet(11, listOf(0, 1, 2, 3, 4));
        PartitionIdSet other = new PartitionIdSet(partitionIdSet);
        assertNotSame(other, partitionIdSet);
        assertContents(other);
    }

    @Test
    public void test_complement() {
        partitionIdSet = new PartitionIdSet(11, listOf(5, 6, 7, 8, 9, 10));
        partitionIdSet.complement();
        assertContents(partitionIdSet);
    }

    @Test
    public void test_union() {
        partitionIdSet.addAll(listOf(0, 2, 4, 6, 8));
        PartitionIdSet other = new PartitionIdSet(271, listOf(1, 3, 5, 7, 9));
        other.union(partitionIdSet);

        for (int i = 0; i < 10; i++) {
            assertTrue(other.contains(i));
        }
        for (int i = 10; i < 271; i++) {
            assertFalse(other.contains(i));
        }
    }

    @Test
    public void test_union_whenSetsOverlap() {
        partitionIdSet = new PartitionIdSet(11, listOf(0, 1, 2, 3, 4));
        PartitionIdSet other = new PartitionIdSet(11, listOf(0, 1, 2));
        partitionIdSet.union(other);
        assertContents(partitionIdSet);
    }

    @Test
    public void test_isMissingPartitions() {
        partitionIdSet.add(1);
        assertTrue(partitionIdSet.isMissingPartitions());

        for (int i = 0; i < 271; i++) {
            partitionIdSet.add(i);
        }
        assertFalse(partitionIdSet.isMissingPartitions());
    }

    @Test
    public void test_intersects() {
        partitionIdSet.add(1);
        PartitionIdSet other = new PartitionIdSet(271, listOf(2, 3, 4));
        assertFalse(partitionIdSet.intersects(other));

        other.add(1);
        assertTrue(partitionIdSet.intersects(other));
    }

    @Test
    public void test_equals() {
        PartitionIdSet set = createWithPartitionCount(10, 1, 2);

        assertEquals(set, set);
        assertEquals(createWithPartitionCount(10, 1, 2), set);
        assertNotEquals(createWithPartitionCount(10, 1, 3), set);
        assertNotEquals(createWithPartitionCount(11, 1, 2), set);
    }

    @Test
    public void test_solePartition() {
        assertEquals(0, createWithPartitionCount(10, 0).solePartition());
        assertEquals(1, createWithPartitionCount(10, 1).solePartition());
        assertEquals(9, createWithPartitionCount(10, 9).solePartition());
        assertEquals(-1, createWithPartitionCount(10, 0, 1).solePartition());
        assertEquals(-1, createWithPartitionCount(10, 0, 9).solePartition());
    }

    private void assertContents(PartitionIdSet set) {
        for (int i = 0; i < 5; i++) {
            assertTrue("Should contain " + i, set.contains(i));
        }
        for (int i = 5; i < 11; i++) {
            assertFalse("Should not contain " + i, set.contains(i));
        }
    }

    private List<Integer> listOf(int... ints) {
        List<Integer> list = new ArrayList<>();
        for (int i : ints) {
            list.add(i);
        }
        return list;
    }

    private static PartitionIdSet create(int... partitions) {
        return createWithPartitionCount(271, partitions);
    }

    private static PartitionIdSet createWithPartitionCount(int partitionCount, int... partitions) {
        PartitionIdSet res = new PartitionIdSet(partitionCount);

        if (partitions != null) {
            for (int partition : partitions) {
                res.add(partition);
            }
        }

        return res;
    }
}
