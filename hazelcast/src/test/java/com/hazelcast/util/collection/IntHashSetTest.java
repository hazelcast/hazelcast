/*
 * Copyright 2015 Real Logic Ltd.
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

package com.hazelcast.util.collection;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class IntHashSetTest {
    private final IntHashSet obj = new IntHashSet(100, -1);

    @Test public void initiallyContainsNoElements() throws Exception {
        for (int i = 0; i < 10000; i++) {
            assertFalse(obj.contains(i));
        }
    }

    @Test public void initiallyContainsNoBoxedElements() {
        for (int i = 0; i < 10000; i++) {
            assertFalse(obj.contains(Integer.valueOf(i)));
        }
    }

    @Test public void containsAddedBoxedElement() {
        assertTrue(obj.add(1));

        assertTrue(obj.contains(1));
    }

    @Test public void addingAnElementTwiceDoesNothing() {
        assertTrue(obj.add(1));

        assertFalse(obj.add(1));
    }

    @Test public void containsAddedBoxedElements() {
        assertTrue(obj.add(1));
        assertTrue(obj.add(Integer.valueOf(2)));

        assertTrue(obj.contains(Integer.valueOf(1)));
        assertTrue(obj.contains(2));
    }

    @Test public void removingAnElementFromAnEmptyListDoesNothing() {
        assertFalse(obj.remove(0));
    }

    @Test public void removingAPresentElementRemovesIt() {
        assertTrue(obj.add(1));

        assertTrue(obj.remove(1));

        assertFalse(obj.contains(1));
    }

    @Test public void sizeIsInitiallyZero() {
        assertEquals(0, obj.size());
    }

    @Test public void sizeIncrementsWithNumberOfAddedElements() {
        obj.add(1);
        obj.add(2);

        assertEquals(2, obj.size());
    }

    @Test public void sizeContainsNumberOfNewElements() {
        obj.add(1);
        obj.add(1);

        assertEquals(1, obj.size());
    }

    @Test public void iteratorsListElements() {
        obj.add(1);
        obj.add(2);

        assertIteratorHasElements();
    }

    @Test public void iteratorsStartFromTheBeginningEveryTime() {
        iteratorsListElements();

        assertIteratorHasElements();
    }

    @Test public void clearRemovesAllElementsOfTheSet() {
        obj.add(1);
        obj.add(2);

        obj.clear();

        assertEquals(0, obj.size());
        assertFalse(obj.contains(1));
        assertFalse(obj.contains(2));
    }

    @Test public void differenceReturnsNullIfBothSetsEqual() {
        obj.add(1);
        obj.add(2);

        final IntHashSet other = new IntHashSet(100, -1);
        other.add(1);
        other.add(2);

        assertNull(obj.difference(other));
    }

    @Test public void differenceReturnsSetDifference() {
        obj.add(1);
        obj.add(2);

        final IntHashSet other = new IntHashSet(100, -1);
        other.add(1);

        final IntHashSet diff = obj.difference(other);
        assertEquals(1, diff.size());
        assertTrue(diff.contains(2));
    }

    @Test public void copiesOtherIntHashSet() {
        obj.add(1);
        obj.add(2);

        final IntHashSet other = new IntHashSet(100, -1);
        other.copy(obj);

        assertThat(other, contains(1, 2));
    }

    @Test public void twoEmptySetsAreEqual() {
        final IntHashSet other = new IntHashSet(100, -1);
        assertEquals(obj, other);
    }

    @Test public void equalityRequiresTheSameMissingValue() {
        final IntHashSet other = new IntHashSet(100, 1);
        assertNotEquals(obj, other);
    }

    @Test public void setsWithTheSameValuesAreEqual() {
        final IntHashSet other = new IntHashSet(100, -1);

        obj.add(1);
        obj.add(1001);

        other.add(1);
        other.add(1001);

        assertEquals(obj, other);
    }

    @Test public void setsWithTheDifferentSizesAreNotEqual() {
        final IntHashSet other = new IntHashSet(100, -1);

        obj.add(1);
        obj.add(1001);

        other.add(1001);

        assertNotEquals(obj, other);
    }

    @Test public void setsWithTheDifferentValuesAreNotEqual() {
        final IntHashSet other = new IntHashSet(100, -1);

        obj.add(1);
        obj.add(1001);

        other.add(2);
        other.add(1001);

        assertNotEquals(obj, other);
    }

    @Test public void twoEmptySetsHaveTheSameHashcode() {
        final IntHashSet other = new IntHashSet(100, -1);
        assertEquals(obj.hashCode(), other.hashCode());
    }

    @Test public void setsWithTheSameValuesHaveTheSameHashcode() {
        final IntHashSet other = new IntHashSet(100, -1);

        obj.add(1);
        obj.add(1001);

        other.add(1);
        other.add(1001);

        assertEquals(obj.hashCode(), other.hashCode());
    }

    private void assertIteratorHasElements() {
        final Iterator<Integer> iter = obj.iterator();

        assertTrue(iter.hasNext());
        assertEquals(Integer.valueOf(1), iter.next());
        assertTrue(iter.hasNext());
        assertEquals(Integer.valueOf(2), iter.next());
        assertFalse(iter.hasNext());
    }
}
