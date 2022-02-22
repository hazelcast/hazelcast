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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.test.ObjectTestUtils;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.query.impl.predicates.PredicateUtils.isNull;
import static com.hazelcast.internal.util.collection.ArrayUtils.createCopy;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * Defines a test base that is useful while testing predicate {@link Visitor
 * visitors}.
 */
public abstract class VisitorTestSupport {

    /**
     * @return the visitor being tested.
     */
    protected abstract Visitor getVisitor();

    /**
     * @return the indexes available to the visitor being tested.
     */
    protected abstract Indexes getIndexes();

    protected static void assertEqualPredicate(EqualPredicate expected, Predicate actual) {
        assertTrue(actual instanceof EqualPredicate);
        EqualPredicate actualEqualPredicate = (EqualPredicate) actual;
        assertEquals(expected.attributeName, actualEqualPredicate.attributeName);
        assertEquals(expected.value, actualEqualPredicate.value);

        assertRangePredicate(expected, actualEqualPredicate);
    }

    protected static AndPredicate and(Predicate... predicates) {
        return new AndPredicate(predicates);
    }

    protected static Predicate same() {
        return new ReferencePredicate(-1);
    }

    protected static Predicate ref(int referentIndex) {
        return new ReferencePredicate(referentIndex);
    }

    protected void check(Predicate expected, Predicate... andPredicates) {
        // https://en.wikipedia.org/wiki/Heap%27s_algorithm

        AndPredicate original = new AndPredicate(andPredicates);
        Predicate[] permutation = createCopy(original.predicates);
        int[] counters = new int[permutation.length];

        assertPredicate(expected, original, getVisitor().visit(original, getIndexes()));

        int i = 0;
        while (i < permutation.length) {
            if (counters[i] < i) {
                if ((i & 1) == 0) {
                    swap(permutation, 0, i);
                } else {
                    swap(permutation, counters[i], i);
                }

                AndPredicate permutedAnd = new AndPredicate(permutation);
                AndPredicate effectiveOriginal = expected instanceof ReferencePredicate ? permutedAnd : original;
                assertPredicate(expected, effectiveOriginal, getVisitor().visit(permutedAnd, getIndexes()));

                ++counters[i];
                i = 0;
            } else {
                counters[i] = 0;
                ++i;
            }
        }
    }

    private static void assertPredicate(Predicate expected, AndPredicate original, Predicate actual) {
        if (expected instanceof AndPredicate) {
            assertAnd((AndPredicate) expected, original, actual);
        } else if (expected instanceof ReferencePredicate) {
            assertSame(((ReferencePredicate) expected).resolve(original), actual);
        } else if (expected instanceof RangePredicate) {
            assertRangePredicate((RangePredicate) expected, actual);
        } else if (expected instanceof CompositeRangePredicate) {
            assertCompositeRangePredicate((CompositeRangePredicate) expected, actual);
        } else if (expected instanceof CompositeEqualPredicate) {
            assertCompositeEqualPredicate((CompositeEqualPredicate) expected, actual);
        } else if (expected instanceof FalsePredicate) {
            assertTrue("expected FalsePredicate got " + actual, actual instanceof FalsePredicate);
        } else {
            fail("unmatched predicate: " + expected);
        }
    }

    private static void assertCompositeEqualPredicate(CompositeEqualPredicate expected, Predicate actual) {
        assertTrue(actual instanceof CompositeEqualPredicate);
        CompositeEqualPredicate actualEqualPredicate = (CompositeEqualPredicate) actual;

        assertEquals(expected.indexName, actualEqualPredicate.indexName);
        assertArrayEquals(expected.components, actualEqualPredicate.components);
        assertEquals(expected.value, actualEqualPredicate.value);
    }

    private static void assertCompositeRangePredicate(CompositeRangePredicate expected, Predicate actual) {
        assertTrue(actual.getClass().toString(), actual instanceof CompositeRangePredicate);
        CompositeRangePredicate actualRangePredicate = (CompositeRangePredicate) actual;

        assertEquals(expected.indexName, actualRangePredicate.indexName);
        assertArrayEquals(expected.components, actualRangePredicate.components);

        assertEquals(expected.from, actualRangePredicate.from);
        assertEquals(expected.fromInclusive, actualRangePredicate.fromInclusive);

        assertEquals(expected.to, actualRangePredicate.to);
        assertEquals(expected.toInclusive, actualRangePredicate.toInclusive);
    }

    private static void assertRangePredicate(RangePredicate expected, Predicate actual) {
        assertEquals(expected.getClass(), actual.getClass());
        assertTrue(actual instanceof RangePredicate);
        RangePredicate actualRangePredicate = (RangePredicate) actual;
        assertEquals(expected.getAttribute(), actualRangePredicate.getAttribute());
        assertTrue("expected " + expected.getFrom() + " got " + actualRangePredicate.getFrom(),
                equalComparable(expected.getFrom(), actualRangePredicate.getFrom()));
        assertEquals(expected.isFromInclusive(), actualRangePredicate.isFromInclusive());
        assertTrue("expected " + expected.getTo() + " got " + actualRangePredicate.getTo(),
                equalComparable(expected.getTo(), actualRangePredicate.getTo()));
        assertEquals(expected.isToInclusive(), actualRangePredicate.isToInclusive());
    }

    private static void assertAnd(AndPredicate expected, AndPredicate original, Predicate actual) {
        assertTrue(actual instanceof AndPredicate);
        AndPredicate optimizedAnd = (AndPredicate) actual;
        assertEquals(expected.predicates.length, optimizedAnd.predicates.length);

        Map<Predicate, Integer> unmatched = new IdentityHashMap<>();
        for (Predicate predicate : optimizedAnd.predicates) {
            Integer count = unmatched.get(predicate);
            if (count == null) {
                count = 0;
            }
            unmatched.put(predicate, count + 1);
        }

        outer:
        for (Predicate predicate : expected.predicates) {
            Predicate expectedPredicate =
                    predicate instanceof ReferencePredicate ? ((ReferencePredicate) predicate).resolve(original) : predicate;

            Integer count = unmatched.get(expectedPredicate);
            if (count != null) {
                --count;
                if (count == 0) {
                    unmatched.remove(expectedPredicate);
                } else {
                    unmatched.put(expectedPredicate, count);
                }
            } else if (expectedPredicate instanceof RangePredicate) {
                for (Iterator<Predicate> i = unmatched.keySet().iterator(); i.hasNext(); ) {
                    Predicate unmatchedPredicate = i.next();
                    if (unmatchedPredicate instanceof RangePredicate && rangePredicatesAreEqual(
                            (RangePredicate) expectedPredicate, (RangePredicate) unmatchedPredicate)) {
                        i.remove();
                        continue outer;
                    }
                }

                fail("unmatched predicate: " + expectedPredicate);
            } else if (expectedPredicate instanceof CompositeRangePredicate) {
                for (Iterator<Predicate> i = unmatched.keySet().iterator(); i.hasNext(); ) {
                    Predicate unmatchedPredicate = i.next();
                    if (unmatchedPredicate instanceof CompositeRangePredicate && compositeRangePredicatesAreEqual(
                            (CompositeRangePredicate) expectedPredicate, (CompositeRangePredicate) unmatchedPredicate)) {
                        i.remove();
                        continue outer;
                    }
                }

                fail("unmatched predicate: " + expectedPredicate);
            } else if (expectedPredicate instanceof CompositeEqualPredicate) {
                for (Iterator<Predicate> i = unmatched.keySet().iterator(); i.hasNext(); ) {
                    Predicate unmatchedPredicate = i.next();
                    if (unmatchedPredicate instanceof CompositeEqualPredicate && compositeEqualPredicatesAreEqual(
                            (CompositeEqualPredicate) expectedPredicate, (CompositeEqualPredicate) unmatchedPredicate)) {
                        i.remove();
                        continue outer;
                    }
                }

                fail("unmatched predicate: " + expectedPredicate);
            } else {
                fail("unmatched predicate: " + expectedPredicate);
            }
        }
    }

    private static <E> void swap(E[] array, int i, int j) {
        E copy = array[i];
        array[i] = array[j];
        array[j] = copy;
    }

    @SuppressWarnings("unchecked")
    private static boolean equalComparable(Comparable lhs, Comparable rhs) {
        return !isNull(lhs) && (lhs == rhs || lhs.compareTo(rhs) == 0) || isNull(rhs);
    }

    private static boolean rangePredicatesAreEqual(RangePredicate lhs, RangePredicate rhs) {
        return lhs.getAttribute().equals(rhs.getAttribute()) && equalComparable(lhs.getFrom(), rhs.getFrom())
                && lhs.isFromInclusive() == rhs.isFromInclusive() && equalComparable(lhs.getTo(), rhs.getTo())
                && lhs.isToInclusive() == rhs.isToInclusive();
    }

    private static boolean compositeRangePredicatesAreEqual(CompositeRangePredicate lhs, CompositeRangePredicate rhs) {
        return ObjectTestUtils.equals(lhs.indexName, rhs.indexName)
            && ObjectTestUtils.equals(lhs.components, rhs.components)
            && lhs.from.equals(rhs.from) && lhs.fromInclusive == rhs.fromInclusive && lhs.to.equals(rhs.to)
            && lhs.toInclusive == rhs.toInclusive;
    }

    private static boolean compositeEqualPredicatesAreEqual(CompositeEqualPredicate lhs, CompositeEqualPredicate rhs) {
        return ObjectTestUtils.equals(lhs.indexName, rhs.indexName)
            && ObjectTestUtils.equals(lhs.components, rhs.components) && lhs.value.equals(rhs.value);
    }

    private static class ReferencePredicate implements Predicate {

        private final int referentIndex;

        ReferencePredicate(int referentIndex) {
            this.referentIndex = referentIndex;
        }

        public Predicate resolve(AndPredicate andPredicate) {
            return referentIndex == -1 ? andPredicate : andPredicate.predicates[referentIndex];
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            throw new UnsupportedOperationException();
        }

    }

    protected static class CustomPredicate implements Predicate {

        @Override
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }

    }

}
