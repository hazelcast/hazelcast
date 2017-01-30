/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import static com.hazelcast.query.Predicates.not;
import static com.hazelcast.query.Predicates.or;
import static com.hazelcast.query.impl.TypeConverters.INTEGER_CONVERTER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FlatteningVisitorTest {

    private FlatteningVisitor visitor;
    private Indexes mockIndexes;
    private Index mockIndex;

    @Before
    public void setUp() {
        mockIndexes = mock(Indexes.class);
        mockIndex = mock(Index.class);
        when(mockIndexes.getIndex(anyString())).thenReturn(mockIndex);
        visitor = new FlatteningVisitor();
        useConverter(INTEGER_CONVERTER);
    }

    @Test
    public void visitAndPredicate_whenHasInnerAndPredicate_thenFlattenIt() {
        // (a1 = 1 and (a2 = 2 and a3 = 3))  -->  (a1 = 1 and a2 = 2 and a3 = 3)

        Predicate a1 = equal("a1", 1);
        Predicate a2 = equal("a2", 2);
        Predicate a3 = equal("a3", 3);

        AndPredicate innerAnd = (AndPredicate) and(a2, a3);
        AndPredicate outerAnd = (AndPredicate) and(a1, innerAnd);

        AndPredicate result = (AndPredicate) visitor.visit(outerAnd, mockIndexes);
        Predicate[] inners = result.predicates;
        assertEquals(3, inners.length);
    }

    @Test
    public void visitOrPredicate_whenHasInnerOrPredicate_thenFlattenIt() {
        // (a1 = 1 or (a2 = 2 or a3 = 3))  -->  (a1 = 1 or a2 = 2 or a3 = 3)

        Predicate a1 = equal("a1", 1);
        Predicate a2 = equal("a2", 2);
        Predicate a3 = equal("a3", 3);

        OrPredicate innerOr = (OrPredicate) or(a2, a3);
        OrPredicate outerOr = (OrPredicate) or(a1, innerOr);

        OrPredicate result = (OrPredicate) visitor.visit(outerOr, mockIndexes);
        Predicate[] inners = result.predicates;
        assertEquals(3, inners.length);
    }

    @Test
    public void visitNotPredicate_whenContainsNegatablePredicate_thenFlattenIt() {
        // (not(equals(foo, 1)))  -->  (notEquals(foo, 1))

        Predicate negated = mock(Predicate.class);
        NegatablePredicate negatablePredicate = mock(NegatablePredicate.class, withSettings().extraInterfaces(Predicate.class));
        when(negatablePredicate.negate()).thenReturn(negated);

        NotPredicate outerPredicate = (NotPredicate) not((Predicate) negatablePredicate);

        Predicate result = visitor.visit(outerPredicate, mockIndexes);
        assertEquals(negated, result);
    }

    private void useConverter(TypeConverter converter) {
        when(mockIndex.getConverter()).thenReturn(converter);
    }

    private void disbledConverter() {
        when(mockIndex.getConverter()).thenReturn(null);
    }
}
