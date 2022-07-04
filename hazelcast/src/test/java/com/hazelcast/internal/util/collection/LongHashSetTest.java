/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.google.common.collect.Lists;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LongHashSetTest {
    @Rule
    public final ExpectedException rule = ExpectedException.none();

    private final LongHashSet set = new LongHashSet(1000, -1);

    @Test
    public void initiallyContainsNoElements() throws Exception {
        for (int i = 0; i < 10000; i++) {
            assertFalse(set.contains(i));
        }
    }

    @Test
    public void initiallyContainsNoBoxedElements() {
        for (int i = 0; i < 10000; i++) {
            assertFalse(set.contains(Long.valueOf(i)));
        }
    }

    @Test
    public void containsAddedBoxedElement() {
        assertTrue(set.add(1));

        assertTrue(set.contains(1));
    }

    @Test
    public void addingAnElementTwiceDoesNothing() {
        assertTrue(set.add(1));

        assertFalse(set.add(1));
    }

    @Test
    public void containsAddedBoxedElements() {
        assertTrue(set.add(1));
        assertTrue(set.add(Long.valueOf(2)));

        assertTrue(set.contains(Long.valueOf(1)));
        assertTrue(set.contains(2));
    }

    @Test
    public void removingAnElementFromAnEmptyListDoesNothing() {
        assertFalse(set.remove(0));
    }

    @Test
    public void removingAPresentElementRemovesIt() {
        final Set<Long> jdkSet = new HashSet<Long>();
        final Random rnd = new Random();
        for (int i = 0; i < 1000; i++) {
            final long value = rnd.nextInt();
            set.add(value);
            jdkSet.add(value);
        }
        assertEquals(jdkSet, set);
        for (Iterator<Long> iter = jdkSet.iterator(); iter.hasNext(); ) {
            final long value = iter.next();
            assertTrue("Set suddenly doesn't contain " + value, set.contains(value));
            assertTrue("Didn't remove " + value, set.remove(value));
            iter.remove();
        }
    }

    @Test
    public void sizeIsInitiallyZero() {
        assertEquals(0, set.size());
    }

    @Test
    public void sizeIncrementsWithNumberOfAddedElements() {
        set.add(1);
        set.add(2);

        assertEquals(2, set.size());
    }

    @Test
    public void sizeContainsNumberOfNewElements() {
        set.add(1);
        set.add(1);

        assertEquals(1, set.size());
    }

    @Test
    public void iteratorsListElements() {
        set.add(1);
        set.add(2);

        assertIteratorHasElements();
    }

    @Test
    public void iteratorsStartFromTheBeginningEveryTime() {
        iteratorsListElements();

        assertIteratorHasElements();
    }

    @Test
    public void clearRemovesAllElementsOfTheSet() {
        set.add(1);
        set.add(2);

        set.clear();

        assertEquals(0, set.size());
        assertFalse(set.contains(1));
        assertFalse(set.contains(2));
    }

    @Test
    public void differenceReturnsNullIfBothSetsEqual() {
        set.add(1);
        set.add(2);

        final LongHashSet other = new LongHashSet(100, -1);
        other.add(1);
        other.add(2);

        assertNull(set.difference(other));
    }

    @Test
    public void differenceReturnsSetDifference() {
        set.add(1);
        set.add(2);

        final LongHashSet other = new LongHashSet(100, -1);
        other.add(1);

        final LongHashSet diff = set.difference(other);
        assertEquals(1, diff.size());
        assertTrue(diff.contains(2));
    }

    @Test
    public void copiesOtherLongHashSet() {
        set.add(1);
        set.add(2);

        final LongHashSet other = new LongHashSet(1000, -1);
        other.copy(set);

        assertThat(other, contains(2L, 1L));
    }

    @Test
    public void twoEmptySetsAreEqual() {
        final LongHashSet other = new LongHashSet(100, -1);
        assertEquals(set, other);
    }

    @Test
    public void equalityRequiresTheSameMissingValue() {
        final LongHashSet other = new LongHashSet(100, 1);
        assertNotEquals(set, other);
    }

    @Test
    public void setsWithTheSameValuesAreEqual() {
        final LongHashSet other = new LongHashSet(100, -1);

        set.add(1);
        set.add(1001);

        other.add(1);
        other.add(1001);

        assertEquals(set, other);
    }

    @Test
    public void setsWithTheDifferentSizesAreNotEqual() {
        final LongHashSet other = new LongHashSet(100, -1);

        set.add(1);
        set.add(1001);

        other.add(1001);

        assertNotEquals(set, other);
    }

    @Test
    public void setsWithTheDifferentValuesAreNotEqual() {
        final LongHashSet other = new LongHashSet(100, -1);

        set.add(1);
        set.add(1001);

        other.add(2);
        other.add(1001);

        assertNotEquals(set, other);
    }

    @Test
    public void twoEmptySetsHaveTheSameHashcode() {
        final LongHashSet other = new LongHashSet(100, -1);
        assertEquals(set.hashCode(), other.hashCode());
    }

    @Test
    public void setsWithTheSameValuesHaveTheSameHashcode() {
        final LongHashSet other = new LongHashSet(100, -1);

        set.add(1);
        set.add(1001);

        other.add(1);
        other.add(1001);

        assertEquals(set.hashCode(), other.hashCode());
    }

    @Test
    public void worksCorrectlyWhenFull() {
        final LongHashSet set = new LongHashSet(2, 0);
        set.add(1);
        set.add(2);
        assertTrue(set.contains(2));
        assertFalse(set.contains(3));
    }

    @Test
    public void failsWhenOverCapacity() {
        final LongHashSet set = new LongHashSet(1, 0);
        set.add(1);
        rule.expect(IllegalStateException.class);
        set.add(2);
    }

    @Test
    public void toArrayReturnsArrayOfAllElement() {
        final LongHashSet initial = new LongHashSet(100, -1);
        initial.add(1);
        initial.add(13);
        final Object[] ary = initial.toArray();
        final Set<Object> fromArray = new HashSet<Object>(Arrays.asList(ary));
        assertEquals(new HashSet<Object>(initial), fromArray);
    }

    @Test
    public void intoArrayReturnsArrayOfAllElement() {
        final LongHashSet initial = new LongHashSet(100, -1);
        initial.add(1);
        initial.add(13);
        final Object[] ary = initial.toArray(new Long[0]);
        final Set<Object> fromArray = new HashSet<Object>(Arrays.asList(ary));
        assertEquals(new HashSet<Object>(initial), fromArray);
    }

    @Test
    public void testLongArrayConstructor() {
        long[] items = {42L, 43L};
        long missingValue = -1L;
        final LongHashSet hashSet = new LongHashSet(items, missingValue);

        set.add(42L);
        set.add(43L);

        assertEquals(set, hashSet);
    }

    @Test
    public void testRemove_whenValuePassedAsObject() {
        final LongHashSet initial = new LongHashSet(100, -1);
        initial.add(42);
        initial.remove(Long.valueOf(42));
        assertFalse(initial.contains(Long.valueOf(42)));
        assertTrue(initial.isEmpty());
    }

    @Test
    public void testAddAll_whenCollectionGiven() {
        Long[] items = {43L, 44L};
        final List<Long> expected = Lists.asList(42L, items);
        final LongHashSet actual = new LongHashSet(100, -1);

        actual.addAll(expected);
        assertEquals(expected.size(), actual.size());
        assertTrue(actual.containsAll(expected));
    }

    @Test
    public void testRemoveAll_whenCollectionGiven() {
        Long[] items = {43L, 44L};
        final List<Long> toBeRemoved = Lists.asList(42L, items);
        final LongHashSet actual = new LongHashSet(100, -1);
        actual.addAll(toBeRemoved);
        assertEquals(toBeRemoved.size(), actual.size());

        actual.removeAll(toBeRemoved);
        assertTrue(actual.isEmpty());
    }

    private void assertIteratorHasElements() {
        final Iterator<Long> iter = set.iterator();

        assertTrue(iter.hasNext());
        assertEquals(Long.valueOf(2), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(Long.valueOf(1), iter.next());
        assertFalse(iter.hasNext());
    }
}
