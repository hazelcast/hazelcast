package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
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
@Category({QuickTest.class, ParallelTest.class})
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
}
