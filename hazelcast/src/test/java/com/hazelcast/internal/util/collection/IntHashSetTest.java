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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IntHashSetTest extends HazelcastTestSupport {

    @Rule
    public final ExpectedException rule = ExpectedException.none();

    private final IntHashSet set = new IntHashSet(1000, -1);

    @Test
    public void initiallyContainsNoElements() throws Exception {
        for (int i = 0; i < 10000; i++) {
            assertNotContains(set, i);
        }
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void initiallyContainsNoBoxedElements() {
        for (int i = 0; i < 10000; i++) {
            assertNotContains(set, Integer.valueOf(i));
        }
    }

    @Test
    public void containsAddedBoxedElement() {
        assertTrue(set.add(1));

        assertContains(set, 1);
    }

    @Test
    public void addingAnElementTwiceDoesNothing() {
        assertTrue(set.add(1));

        assertFalse(set.add(1));
    }

    @Test
    @SuppressWarnings("UnnecessaryBoxing")
    public void containsAddedBoxedElements() {
        assertTrue(set.add(1));
        assertTrue(set.add(Integer.valueOf(2)));

        assertContains(set, Integer.valueOf(1));
        assertContains(set, 2);
    }

    @Test
    public void removingAnElementFromAnEmptyListDoesNothing() {
        assertFalse(set.remove(0));
    }

    @Test
    public void removingAPresentElementRemovesIt() {
        final Set<Integer> jdkSet = new HashSet<Integer>();
        final Random rnd = new Random();
        for (int i = 0; i < 1000; i++) {
            final int value = rnd.nextInt();
            set.add(value);
            jdkSet.add(value);
        }
        assertEquals(jdkSet, set);
        for (Iterator<Integer> iter = jdkSet.iterator(); iter.hasNext(); ) {
            final int value = iter.next();
            assertContains(set, value);
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
        assertNotContains(set, 1);
        assertNotContains(set, 2);
    }

    @Test
    public void differenceReturnsNullIfBothSetsEqual() {
        set.add(1);
        set.add(2);

        final IntHashSet other = new IntHashSet(100, -1);
        other.add(1);
        other.add(2);

        assertNull(set.difference(other));
    }

    @Test
    public void differenceReturnsSetDifference() {
        set.add(1);
        set.add(2);

        final IntHashSet other = new IntHashSet(100, -1);
        other.add(1);

        final IntHashSet diff = set.difference(other);
        assertEquals(1, diff.size());
        assertContains(diff, 2);
    }

    @Test
    public void copiesOtherIntHashSet() {
        set.add(1);
        set.add(2);

        final IntHashSet other = new IntHashSet(1000, -1);
        other.copy(set);

        assertContains(other, 1);
        assertContains(other, 2);
    }

    @Test
    public void twoEmptySetsAreEqual() {
        final IntHashSet other = new IntHashSet(100, -1);
        assertEquals(set, other);
    }

    @Test
    public void equalityRequiresTheSameMissingValue() {
        final IntHashSet other = new IntHashSet(100, 1);
        assertNotEquals(set, other);
    }

    @Test
    public void setsWithTheSameValuesAreEqual() {
        final IntHashSet other = new IntHashSet(100, -1);

        set.add(1);
        set.add(1001);

        other.add(1);
        other.add(1001);

        assertEquals(set, other);
    }

    @Test
    public void setsWithTheDifferentSizesAreNotEqual() {
        final IntHashSet other = new IntHashSet(100, -1);

        set.add(1);
        set.add(1001);

        other.add(1001);

        assertNotEquals(set, other);
    }

    @Test
    public void setsWithTheDifferentValuesAreNotEqual() {
        final IntHashSet other = new IntHashSet(100, -1);

        set.add(1);
        set.add(1001);

        other.add(2);
        other.add(1001);

        assertNotEquals(set, other);
    }

    @Test
    public void twoEmptySetsHaveTheSameHashcode() {
        final IntHashSet other = new IntHashSet(100, -1);
        assertEquals(set.hashCode(), other.hashCode());
    }

    @Test
    public void setsWithTheSameValuesHaveTheSameHashcode() {
        final IntHashSet other = new IntHashSet(100, -1);

        set.add(1);
        set.add(1001);

        other.add(1);
        other.add(1001);

        assertEquals(set.hashCode(), other.hashCode());
    }

    @Test
    public void worksCorrectlyWhenFull() {
        final IntHashSet set = new IntHashSet(2, 0);
        set.add(1);
        set.add(2);
        assertContains(set, 2);
        assertNotContains(set, 3);
    }

    @Test
    public void failsWhenOverCapacity() {
        final IntHashSet set = new IntHashSet(1, 0);
        set.add(1);
        rule.expect(IllegalStateException.class);
        set.add(2);
    }

    @Test
    public void toArrayReturnsArrayOfAllElement() {
        final IntHashSet initial = new IntHashSet(100, -1);
        initial.add(1);
        initial.add(13);
        final Object[] ary = initial.toArray();
        final Set<Object> fromArray = new HashSet<Object>(Arrays.asList(ary));
        assertEquals(new HashSet<Object>(initial), fromArray);
    }

    @Test
    public void intoArrayReturnsArrayOfAllElement() {
        final IntHashSet initial = new IntHashSet(100, -1);
        initial.add(1);
        initial.add(13);
        final Object[] ary = initial.toArray(new Integer[0]);
        final Set<Object> fromArray = new HashSet<Object>(Arrays.asList(ary));
        assertEquals(new HashSet<Object>(initial), fromArray);
    }

    private void assertIteratorHasElements() {
        final Iterator<Integer> iter = set.iterator();

        assertTrue(iter.hasNext());
        assertEquals(Integer.valueOf(2), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(Integer.valueOf(1), iter.next());
        assertFalse(iter.hasNext());
    }
}
