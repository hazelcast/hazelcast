package com.hazelcast.query.impl.predicates;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BetweenPredicateTest {

    @Test
    @SuppressWarnings("unchecked")
    // should be fixed in 4.0; See: https://github.com/hazelcast/hazelcast/issues/6188
    public void equal_zeroMinusZero() {
        // in BetweenPredicate
        assertFalse(new BetweenPredicate("this", 0.0, 0.0).apply(entry(-0.0)));
        assertFalse(new BetweenPredicate("this", 0.0d, 0.0d).apply(entry(-0.0d)));
        assertFalse(new BetweenPredicate("this", 0.0, 0.0).apply(entry(-0.0d)));
        assertFalse(new BetweenPredicate("this", 0.0d, 0.0d).apply(entry(-0.0)));

        // whereas in Java
        assertTrue(0.0 == -0.0);
        assertTrue(0.0d == -0.0d);
        assertTrue(0.0 == -0.0d);
        assertTrue(0.0d == -0.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    // should be fixed in 4.0; See: https://github.com/hazelcast/hazelcast/issues/6188
    public void equal_NaN() {
        // in BetweenPredicate
        assertTrue(new BetweenPredicate("this", Double.NaN, Double.NaN).apply(entry(Double.NaN)));
        assertTrue(new BetweenPredicate("this", Float.NaN, Float.NaN).apply(entry(Float.NaN)));
        assertTrue(new BetweenPredicate("this", Double.NaN, Double.NaN).apply(entry(-Double.NaN)));
        assertTrue(new BetweenPredicate("this", Float.NaN, Float.NaN).apply(entry(-Float.NaN)));
        assertTrue(new BetweenPredicate("this", Double.NaN, Double.NaN).apply(entry(-Float.NaN)));
        assertTrue(new BetweenPredicate("this", Float.NaN, Float.NaN).apply(entry(-Double.NaN)));

        // whereas in Java
        assertFalse(Double.NaN == Double.NaN);
        assertFalse(Float.NaN == Float.NaN);
        assertFalse(Double.NaN == -Double.NaN);
        assertFalse(Float.NaN == -Float.NaN);
        assertFalse(Double.NaN == -Float.NaN);
        assertFalse(Float.NaN == -Double.NaN);
    }
}
