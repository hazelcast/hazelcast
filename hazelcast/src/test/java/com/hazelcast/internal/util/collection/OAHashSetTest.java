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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OAHashSetTest {

    @Test
    public void testConstructorLoadFactorIsUsed() {
        final float loadFactor = 0.9F;
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8, loadFactor);

        assertEquals(loadFactor, set.loadFactor(), 10E-9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNegativeInitialCapacity() {
        new OAHashSet<Integer>(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorIllegalLoadFactorZero() {
        new OAHashSet<Integer>(8, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorIllegalLoadFactorOne() {
        new OAHashSet<Integer>(8, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorNanLoadFactor() {
        new OAHashSet<Integer>(8, Float.NaN);
    }

    @Test(expected = NullPointerException.class)
    public void testAddNull() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.add(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddWithHashNull() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.add(null, 1);
    }

    @Test
    public void testAdd() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);

        for (int i = 0; i < 10; i++) {
            final boolean added = set.add(i);
            assertTrue("Element " + i + " should be added", added);
        }
    }

    @Test
    public void testAddHashCollision() {
        final OAHashSet<IntegerWithFixedHashCode> set = new OAHashSet<IntegerWithFixedHashCode>(8);

        for (int i = 0; i < 10; i++) {
            final boolean added = set.add(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be added", added);
        }

        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test
    public void testAddingAlreadyContainedElementDoesNotChangeTheSet() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        final boolean addedFive = set.add(5);
        assertFalse("Element 5 should not be added twice", addedFive);

        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test
    public void testAddIncreasesSize() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);

        for (int i = 0; i < 10; i++) {
            final boolean added = set.add(i);
            assertTrue("Element " + i + " should be added", added);
        }
        assertEquals(10, set.size());

        final boolean added = set.add(10);
        assertTrue("Element 10 should be added", added);
        assertEquals(11, set.size());
    }

    @Test
    public void testAddWithHashCollisionIncreasesSize() {
        final OAHashSet<IntegerWithFixedHashCode> set = new OAHashSet<IntegerWithFixedHashCode>(8);

        for (int i = 0; i < 10; i++) {
            final boolean added = set.add(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be added", added);
        }
        assertEquals(10, set.size());

        final boolean added = set.add(new IntegerWithFixedHashCode(10));
        assertTrue("Element 10 should be added", added);
        assertEquals(11, set.size());
    }

    @Test
    public void testAddAll() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);

        Collection<Integer> elementsToAdd = new ArrayList<Integer>(10);
        for (int i = 0; i < 10; i++) {
            elementsToAdd.add(i);
        }
        set.addAll(elementsToAdd);

        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testAddAllThrowsOnNullElement() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);

        final Collection<Integer> elementsToAdd = new ArrayList<Integer>(2);
        elementsToAdd.add(1);
        elementsToAdd.add(null);

        set.addAll(elementsToAdd);
    }

    @Test
    public void testClear() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }

        set.clear();

        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(i);
            assertFalse("Element " + i + " should not be contained", contained);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testContainsNull() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.contains(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsWithHashNull() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.contains(null, 1);
    }

    @Test
    public void testContains() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test
    public void testContainsReturnsFalseIfElementNotAdded() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);

        assertFalse(set.contains(5));
    }

    @Test
    public void testContainsAll() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        Collection<Integer> elementsToCheck = new ArrayList<Integer>(10);
        for (int i = 0; i < 10; i++) {
            elementsToCheck.add(i);
        }

        final boolean containedAll = set.containsAll(elementsToCheck);
        assertTrue("All elements should be contained", containedAll);
    }

    @Test
    public void testContainsAllWithElementNotAdded() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        Collection<Integer> elementsToCheck = new ArrayList<Integer>(11);
        for (int i = 0; i < 11; i++) {
            elementsToCheck.add(i);
        }

        final boolean containedAll = set.containsAll(elementsToCheck);
        assertFalse("Not all elements are expected to be contained", containedAll);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsAllThrowsOnNullElement() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.add(1);

        final Collection<Integer> elementsToCheck = new ArrayList<Integer>(2);
        elementsToCheck.add(1);
        elementsToCheck.add(null);

        set.containsAll(elementsToCheck);
    }

    @Test
    public void testCapacityIncreasesIfNeeded() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8, 0.9F);
        assertEquals(8, set.capacity());

        populateSet(set, 10);

        assertEquals(16, set.capacity());
    }

    @Test
    public void testIsEmpty() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        assertTrue("Set should be empty", set.isEmpty());

        set.add(1);
        assertFalse("Set should not be empty", set.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveNull() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.remove(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveWithHashNull() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.remove(null, 1);
    }

    @Test
    public void testRemove() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        final boolean removed = set.remove(5);

        assertTrue(removed);
        assertFalse("Element 5 should not be contained", set.contains(5));
        for (int i = 0; i < 5; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }
        for (int i = 6; i < 10; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test
    public void testRemoveHashCollision() {
        final OAHashSet<IntegerWithFixedHashCode> set = new OAHashSet<IntegerWithFixedHashCode>(8);

        for (int i = 0; i < 10; i++) {
            final boolean added = set.add(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be added", added);
        }

        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be contained", contained);
        }

        final IntegerWithFixedHashCode elementToRemove = new IntegerWithFixedHashCode(5);
        final boolean removed = set.remove(elementToRemove);

        assertTrue(removed);
        assertFalse("Element " + elementToRemove + " should not be contained", set.contains(elementToRemove));
        for (int i = 0; i < 5; i++) {
            final boolean contained = set.contains(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be contained", contained);
        }
        for (int i = 6; i < 10; i++) {
            final boolean contained = set.contains(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test
    public void testRemoveNotAddedElementRemovesNothing() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        final boolean removed = set.remove(42);

        assertFalse(removed);
        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test
    public void testRemoveNotAddedElementRemovesNothingOnHashCollision() {
        final OAHashSet<IntegerWithFixedHashCode> set = new OAHashSet<IntegerWithFixedHashCode>(8);

        for (int i = 0; i < 10; i++) {
            final boolean added = set.add(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be added", added);
        }

        final boolean removed = set.remove(new IntegerWithFixedHashCode(42));

        assertFalse(removed);
        for (int i = 0; i < 10; i++) {
            final boolean contained = set.contains(new IntegerWithFixedHashCode(i));
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test
    public void testRemoveAll() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        Collection<Integer> elementsToRemove = new ArrayList<Integer>(10);
        for (int i = 0; i < 10; i++) {
            elementsToRemove.add(i);
        }

        final boolean removedAll = set.removeAll(elementsToRemove);
        assertTrue("All elements should be removed", removedAll);
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRemoveAllWithElementNotAdded() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        Collection<Integer> elementsToRemove = new ArrayList<Integer>(10);
        for (int i = 5; i < 15; i++) {
            elementsToRemove.add(i);
        }

        final boolean removedAll = set.removeAll(elementsToRemove);
        assertTrue("Elements should be removed from the set", removedAll);
        assertEquals(5, set.size());

        for (int i = 0; i < 5; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }

        for (int i = 5; i < 15; i++) {
            final boolean contained = set.contains(i);
            assertFalse("Element " + i + " should not be contained", contained);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAllThrowsOnNullElement() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 1);

        final Collection<Integer> elementsToRemove = new ArrayList<Integer>(2);
        elementsToRemove.add(1);
        elementsToRemove.add(null);

        set.removeAll(elementsToRemove);
    }

    @Test
    public void testRetainAll() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        Collection<Integer> elementsToRetain = new ArrayList<Integer>(10);
        for (int i = 0; i < 10; i++) {
            elementsToRetain.add(i);
        }

        final boolean retainedAll = set.retainAll(elementsToRetain);
        assertFalse("No elements should be removed", retainedAll);
        assertFalse(set.isEmpty());
    }

    @Test
    public void testRetainAllWithElementNotAdded() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        Collection<Integer> elementsToRetain = new ArrayList<Integer>(10);
        for (int i = 5; i < 15; i++) {
            elementsToRetain.add(i);
        }

        final boolean containedAll = set.retainAll(elementsToRetain);
        assertTrue("Elements should be removed from the set", containedAll);
        assertEquals(5, set.size());

        for (int i = 0; i < 5; i++) {
            final boolean contained = set.contains(i);
            assertFalse("Element " + i + " should not be contained", contained);
        }

        for (int i = 5; i < 10; i++) {
            final boolean contained = set.contains(i);
            assertTrue("Element " + i + " should be contained", contained);
        }
    }

    @Test
    public void testFootprintReflectsCapacityIncrease() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        final long originalFootprint = set.footprint();

        populateSet(set, 10);

        final long footprintAfterCapacityIncrease = set.footprint();
        assertTrue(footprintAfterCapacityIncrease > originalFootprint);
    }

    @Test
    public void testIterator() {
        final int elementCount = 10;
        final BitSet foundElements = new BitSet(elementCount);
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, elementCount);

        for (int stored : set) {
            foundElements.set(stored);
        }

        for (int i = 0; i < elementCount; i++) {
            assertTrue(foundElements.get(i));
        }
    }

    @Test
    public void testIteratorHasNextReturnsTrueIfNotIteratedOverAll() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 2);

        final Iterator<Integer> iterator = set.iterator();
        iterator.next();
        assertTrue(iterator.hasNext());
    }

    @Test
    public void testIteratorHasNextReturnsFalseIfIteratedOverAll() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 1);

        final Iterator<Integer> iterator = set.iterator();
        iterator.next();
        assertFalse(iterator.hasNext());
    }

    @Test(expected = ConcurrentModificationException.class)
    public void testIteratorNextThrowsAfterAdd() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.add(1);

        Iterator<Integer> iterator = set.iterator();
        set.add(2);

        iterator.next();
    }

    @Test(expected = ConcurrentModificationException.class)
    public void testIteratorNextThrowsAfterRemove() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.add(1);

        final Iterator<Integer> iterator = set.iterator();
        set.remove(1);

        iterator.next();
    }

    @Test(expected = ConcurrentModificationException.class)
    public void testIteratorNextThrowsAfterClear() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.add(1);

        final Iterator<Integer> iterator = set.iterator();
        set.clear();

        iterator.next();
    }

    @Test
    public void testIteratorNextThrowsIteratedOverAll() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        set.add(1);

        final Iterator<Integer> iterator = set.iterator();
        iterator.next();
        try {
            iterator.next();
            fail("NoSuchElementException is expected");
        } catch (NoSuchElementException expected) {
            // nothing to do
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorRemoveThrows() {
        new OAHashSet<Integer>(8).iterator().remove();

    }

    @Test
    public void testCompactionDoesNotMoveUnexpectedElements() {
        final OAHashSet<IntegerWithFixedHashCode> set = new OAHashSet<IntegerWithFixedHashCode>(8);

        // we add 3 elements:
        // 4 with hash 4
        // 5 with hash 4 <- would be put into the same array slot than 4 because of its hash, this element will be "compacted"
        // 6 with hash 6
        final IntegerWithFixedHashCode element4 = new IntegerWithFixedHashCode(4, 4);
        final IntegerWithFixedHashCode element5 = new IntegerWithFixedHashCode(5, 4);
        final IntegerWithFixedHashCode element6 = new IntegerWithFixedHashCode(6, 6);
        set.add(element4);
        set.add(element5);
        set.add(element6);
        // we have this table now
        // ...
        // table[4] = 4
        // table[5] = 5
        // table[6] = 6
        // ...

        // we remove 4 with hash 4 -> element 5 with hash 4 moves to index 4 due to compaction
        set.remove(element4);
        // we have this table now
        // ...
        // table[4] = 5
        // table[5] = null
        // table[6] = 6
        // ...

        // without compaction 5 would not be found
        assertTrue(set.contains(element5));
        // 6 would not be found if compaction moved that as well
        assertTrue(set.contains(element6));
    }

    @Test
    public void testToObjectArray() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        final Object[] setElements = set.toArray();
        assertEquals(10, setElements.length);

        final BitSet foundElements = new BitSet(10);
        for (Object foundElement : setElements) {
            foundElements.set((Integer) foundElement);
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(foundElements.get(i));
        }
    }

    @Test
    public void testToGenericArray() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        final Integer[] setElementsProvided = new Integer[10];
        final Integer[] setElementsReturned = set.toArray(setElementsProvided);

        assertSame(setElementsProvided, setElementsReturned);

        final BitSet foundElements = new BitSet(10);
        for (Integer foundElement : setElementsProvided) {
            foundElements.set(foundElement);
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(foundElements.get(i));
        }
    }

    @Test
    public void testToGenericArrayReturnsNewArrayWhenSmallArrayProvided() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        final Integer[] setElementsProvided = new Integer[9];
        final Object[] setElementsReturned = set.toArray(setElementsProvided);

        assertNotSame(setElementsProvided, setElementsReturned);

        final BitSet foundElements = new BitSet(10);
        for (Object foundElement : setElementsReturned) {
            foundElements.set((Integer) foundElement);
        }

        for (int i = 0; i < 10; i++) {
            assertTrue(foundElements.get(i));
        }
    }

    @Test
    public void testToGenericArraySetsNullAfterLastContainedElement() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        final Integer[] setElementsProvided = new Integer[11];
        final Integer[] setElementsReturned = set.toArray(setElementsProvided);
        assertSame(setElementsProvided, setElementsReturned);

        assertNull(setElementsProvided[10]);
    }

    @Test
    public void testHashCode() {
        final OAHashSet<Integer> set = new OAHashSet<Integer>(8);
        populateSet(set, 10);

        final int expectedHashCode = 45;

        assertEquals(expectedHashCode, set.hashCode());

    }

    private void populateSet(OAHashSet<Integer> set, int elementCount) {
        for (int i = 0; i < elementCount; i++) {
            boolean added = set.add(i);
            assertTrue("Element " + i + " should be added", added);
        }
    }

    private static class IntegerWithFixedHashCode {
        private final int value;
        private final int fixedHashCode;

        private IntegerWithFixedHashCode(int value) {
            this(value, 5);
        }

        private IntegerWithFixedHashCode(int value, int fixedHashCode) {
            this.value = value;
            this.fixedHashCode = fixedHashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            IntegerWithFixedHashCode that = (IntegerWithFixedHashCode) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return fixedHashCode;
        }
    }

}
