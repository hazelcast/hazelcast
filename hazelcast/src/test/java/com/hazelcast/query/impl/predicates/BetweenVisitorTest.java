/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.impl.TypeConverters.INTEGER_CONVERTER;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BetweenVisitorTest {

    private BetweenVisitor visitor;
    private Indexes mockIndexes;
    private Index mockIndex;

    @Before
    public void setUp() {
        mockIndexes = mock(Indexes.class);
        mockIndex = mock(Index.class);
        when(mockIndexes.getIndex(anyString())).thenReturn(mockIndex);
        visitor = new BetweenVisitor();
        useConverter(INTEGER_CONVERTER);
    }

    @Test
    public void whenGreaterOrEqualsThanXandLessOrEqualsThanY_thenRewriteToBetweenXandY() {
        //(age >= 5 and age <= 6)  --> (age between 5 6)
        Predicate left = greaterEqual("attribute", 5);
        Predicate right = lessEqual("attribute", 6);
        Predicate and = and(left, right);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertBetweenPredicate(result, 5, 6);
    }

    @Test
    public void whenPredicatesOutsideTheRangeFromRightExist_thenEliminateThem() {
        //(age >= 5 and age <= 6 and age < 10)  -->  (age between 5 6)
        Predicate left = greaterEqual("attribute", 5);
        Predicate right = lessEqual("attribute", 6);
        Predicate other = lessEqual("attribute", 10);
        Predicate and = and(left, right, other);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertBetweenPredicate(result, 5, 6);
    }

    @Test
    public void whenPredicatesOutsideTheRangeFromLeftExist_thenEliminateThem() {
        //(age >= 5 and age <= 6 and age > 4)  -->  (age between 5 6)
        Predicate left = greaterEqual("attribute", 5);
        Predicate right = lessEqual("attribute", 6);
        Predicate other = greaterEqual("attribute", 4);
        Predicate and = and(left, right, other);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertBetweenPredicate(result, 5, 6);
    }

    @Test
    public void whenBothBoundariesAreSame_thenRewriteItAsEquals() {
        //(age >= 5 and age <= 5)  -->  (age = 5)
        Predicate left = greaterEqual("attribute", 5);
        Predicate right = lessEqual("attribute", 5);
        Predicate and = and(left, right);

        EqualPredicate p = (EqualPredicate) visitor.visit((AndPredicate) and, mockIndexes);
        assertEquals("attribute", p.attributeName);
        assertEquals(5, p.value);
    }

    @Test
    public void whenPredicatesOtherThenGreatLess_thenDoNotAttemptToEliminateThem() {
        //(age >= 5 and age <= 6 and age <> 4)  -->  ( (age between 5 6) and (age <> 5) )
        Predicate left = greaterEqual("attribute", 5);
        Predicate right = lessEqual("attribute", 6);
        Predicate other = notEqual("attribute", 4);
        Predicate and = and(left, right, other);

        AndPredicate result = (AndPredicate) visitor.visit((AndPredicate) and, mockIndexes);
        Predicate[] inners = result.predicates;
        assertThat(inners, hasItemInArray(other));
        BetweenPredicate betweenPredicate = findFirstBetweenPredicate(inners);
        assertBetweenPredicate(betweenPredicate, 5, 6);
    }

    @Test
    public void whenPredicateIsExclusive_thenIsNotUsedToBuildBetween() {
        //(age >= 5 and age < 6)  -->  (age >= 5 and age < 6)
        Predicate left = greaterThan("attribute", 5);
        Predicate right = lessEqual("attribute", 6);
        Predicate and = and(left, right);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertSame(and, result);
    }

    @Test
    public void whenPredicateIsNotGreaterLessPredicate_thenIsNotUsedToBuildBetween() {
        Predicate left = equal("attribute", 5);
        Predicate right = lessEqual("attribute", 6);
        Predicate and = and(left, right);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertSame(and, result);
    }

    @Test
    public void whenGreatesOrEqualsThanXandLessOrEqualsThanYAndSomeOtherPredicate_thenRewriteToBetweenXandYAndSomeOtherPredicate() {
        Predicate left = greaterEqual("attribute", 5);
        Predicate right = lessEqual("attribute", 6);
        Predicate other = lessEqual("otherAttribute", 6);
        Predicate and = and(left, right, other);

        AndPredicate result = (AndPredicate) visitor.visit((AndPredicate) and, mockIndexes);

        Predicate[] innerPredicates = result.predicates;
        assertThat(innerPredicates, hasItemInArray(other));
        BetweenPredicate betweenPredicate = findFirstBetweenPredicate(innerPredicates);
        assertBetweenPredicate(betweenPredicate, 5, 6);
    }

    @Test
    public void whenNoConverterExist_thenReturnOriginalPredicate() {
        disableConverter();
        Predicate left = greaterEqual("attribute", 5);
        Predicate right = lessEqual("attribute", 6);
        Predicate and = and(left, right);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertSame(and, result);
    }

    @Test
    public void whenOnlyOneSideFound_thenReturnOriginalPredicate() {
        Predicate p1 = greaterEqual("attribute1", 5);
        Predicate p2 = lessEqual("attribute2", 6);
        Predicate and = and(p1, p2);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertSame(and, result);
    }

    @Test
    public void whenOnlyOneBoundaryFound_thenReturnOriginalPredicate() {
        Predicate p1 = greaterEqual("attribute1", 5);
        Predicate p2 = greaterEqual("attribute1", 6);
        Predicate and = and(p1, p2);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertSame(and, result);
    }

    @Test
    public void whenSideAreOverlapping_thenReturnFalsePredicate() {
        Predicate p1 = greaterEqual("attribute1", 5);
        Predicate p2 = lessEqual("attribute1", 4);
        Predicate and = and(p1, p2);

        Predicate result = visitor.visit((AndPredicate) and, mockIndexes);
        assertEquals(FalsePredicate.INSTANCE, result);
    }

    private void useConverter(TypeConverter converter) {
        when(mockIndex.getConverter()).thenReturn(converter);
    }

    private void disableConverter() {
        when(mockIndex.getConverter()).thenReturn(null);
    }

    private void assertBetweenPredicate(Predicate p, Comparable from, Comparable to) {
        assertEquals(BetweenPredicate.class, p.getClass());
        BetweenPredicate bp = (BetweenPredicate) p;
        assertEquals(from, bp.from);
        assertEquals(to, bp.to);
    }

    private BetweenPredicate findFirstBetweenPredicate(Predicate[] innerPredicates) {
        for (Predicate p : innerPredicates) {
            if (p instanceof BetweenPredicate) {
                return (BetweenPredicate) p;
            }
        }
        return null;
    }
}
