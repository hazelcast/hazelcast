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

import java.util.Iterator;
import java.util.PrimitiveIterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ImmutablePartitionIdSetTest {

    private ImmutablePartitionIdSet set;

    @Before
    public void setup() {
        set = new ImmutablePartitionIdSet(11, asList(0, 1, 2, 3, 4));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_add() {
        set.add(5);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_addInteger() {
        set.add(Integer.valueOf(5));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_addAllCollection() {
        set.addAll(asList(5, 6, 7));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_addAllPartitionIdSet() {
        set.addAll(new PartitionIdSet(11, asList(6, 8, 9)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove() {
        set.remove(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_removeInteger() {
        set.remove(Integer.valueOf(3));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_removeAllCollection() {
        set.removeAll(asList(1, 2, 3));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_removeAllPartitionIdSet() {
        set.removeAll(new PartitionIdSet(11, asList(5, 6, 7)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_removeIf() {
        set.removeIf(partitionId -> partitionId == 2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_clear() {
        set.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_union() {
        set.union(new PartitionIdSet(11, asList(6, 7, 8)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_complement() {
        set.complement();
    }

    @Test
    public void test_intersects() {
        PartitionIdSet set1 = new PartitionIdSet(11, asList(1, 2));
        PartitionIdSet set2 = new PartitionIdSet(11, asList(8, 9));
        assertTrue(set.intersects(set1));
        assertFalse(set.intersects(set2));

        assertTrue(set1.intersects(set));
        assertFalse(set2.intersects(set));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_iteratorRemove() {
        Iterator<Integer> iterator = set.iterator();
        iterator.next();
        iterator.remove();
    }

    @Test
    public void test_contains() {
        assertTrue(set.contains(1));
        assertTrue(set.contains(Integer.valueOf(2)));
    }

    @Test
    public void test_iterator() {
        PrimitiveIterator.OfInt iterator = set.intIterator();
        int i = 0;
        while (iterator.hasNext()) {
            assertEquals(i, iterator.nextInt());
            i++;
        }
    }
}
