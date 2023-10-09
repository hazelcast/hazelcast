/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.createDelegatingVisitor;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.createMockNegatablePredicate;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.createMockVisitablePredicate;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.createPassthroughVisitor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;


@SuppressWarnings("rawtypes")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AndPredicateTest {

    @Test
    public void negate_whenContainsNegatablePredicate_thenReturnOrPredicateWithNegationInside() {
        // ~(foo and bar)  -->  (~foo or ~bar)
        // this is testing the case where the inner predicate implements {@link Negatable}

        Predicate negated = mock(Predicate.class);
        Predicate negatable = createMockNegatablePredicate(negated);

        AndPredicate and = (AndPredicate) and(negatable);
        OrPredicate result = (OrPredicate) and.negate();

        Predicate[] inners = result.predicates;
        assertThat(inners).hasSize(1);
        assertThat(inners).containsExactlyInAnyOrder(negated);
    }

    @Test
    public void negate_whenContainsNonNegatablePredicate_thenReturnOrPredicateWithNotInside() {
        // ~(foo and bar)  -->  (~foo or ~bar)
        // this is testing the case where the inner predicate does NOT implement {@link Negatable}

        Predicate nonNegatable = mock(Predicate.class);

        AndPredicate and = (AndPredicate) and(nonNegatable);
        OrPredicate result = (OrPredicate) and.negate();

        Predicate[] inners = result.predicates;
        assertThat(inners).hasSize(1);

        NotPredicate notPredicate = (NotPredicate) inners[0];
        assertThat(nonNegatable).isSameAs(notPredicate.predicate);
    }

    @Test
    public void accept_whenEmptyPredicate_thenReturnItself() {
        Visitor mockVisitor = createPassthroughVisitor();
        IndexRegistry mockIndexes = mock(IndexRegistry.class);

        AndPredicate andPredicate = new AndPredicate(new Predicate[0]);
        AndPredicate result = (AndPredicate) andPredicate.accept(mockVisitor, mockIndexes);

        assertThat(result).isSameAs(andPredicate);
    }

    @Test
    public void accept_whenInnerPredicateChangedOnAccept_thenReturnAndNewAndPredicate() {
        Visitor mockVisitor = createPassthroughVisitor();
        IndexRegistry mockIndexes = mock(IndexRegistry.class);

        Predicate transformed = mock(Predicate.class);
        Predicate innerPredicate = createMockVisitablePredicate(transformed);
        Predicate[] innerPredicates = new Predicate[1];
        innerPredicates[0] = innerPredicate;

        AndPredicate andPredicate = new AndPredicate(innerPredicates);
        AndPredicate result = (AndPredicate) andPredicate.accept(mockVisitor, mockIndexes);

        assertThat(result).isNotSameAs(andPredicate);
        Predicate[] newInnerPredicates = result.predicates;
        assertThat(newInnerPredicates).hasSize(1);
        assertThat(newInnerPredicates[0]).isEqualTo(transformed);
    }

    @Test
    public void accept_whenVisitorReturnsNewInstance_thenReturnTheNewInstance() {
        Predicate delegate = mock(Predicate.class);
        Visitor mockVisitor = createDelegatingVisitor(delegate);
        IndexRegistry mockIndexes = mock(IndexRegistry.class);
        Predicate innerPredicate = mock(Predicate.class);
        Predicate[] innerPredicates = new Predicate[1];
        innerPredicates[0] = innerPredicate;

        AndPredicate andPredicate = new AndPredicate(innerPredicates);
        Predicate result = andPredicate.accept(mockVisitor, mockIndexes);

        assertThat(result).isSameAs(delegate);
    }
}
