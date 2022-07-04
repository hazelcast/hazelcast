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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.Predicates.or;
import static com.hazelcast.query.impl.predicates.PredicateTestUtils.createMockNegatablePredicate;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OrPredicateTest {

    @Test
    public void negate_whenContainsNegatablePredicate_thenReturnAndPredicateWithNegationInside() {
        // ~(foo or bar)  -->  (~foo and ~bar)
        // this is testing the case where the inner predicate implements {@link Negatable}

        Predicate negated = mock(Predicate.class);
        Predicate negatable = createMockNegatablePredicate(negated);

        OrPredicate or = (OrPredicate) or(negatable);
        AndPredicate result = (AndPredicate) or.negate();

        Predicate[] inners = result.predicates;
        assertThat(inners, arrayWithSize(1));
        assertThat(inners, arrayContainingInAnyOrder(negated));
    }

    @Test
    public void negate_whenContainsNonNegatablePredicate_thenReturnAndPredicateWithNotInside() {
        // ~(foo or bar)  -->  (~foo and ~bar)
        // this is testing the case where the inner predicate does NOT implement {@link Negatable}

        Predicate nonNegatable = mock(Predicate.class);

        OrPredicate or = (OrPredicate) or(nonNegatable);
        AndPredicate result = (AndPredicate) or.negate();

        Predicate[] inners = result.predicates;
        assertThat(inners, arrayWithSize(1));

        NotPredicate notPredicate = (NotPredicate) inners[0];
        assertThat(nonNegatable, sameInstance(notPredicate.predicate));
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(OrPredicate.class)
            .suppress(Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
            .verify();
    }

}
