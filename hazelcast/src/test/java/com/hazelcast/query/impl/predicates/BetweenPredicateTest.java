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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(BetweenPredicate.class)
            .suppress(Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
            .withRedefinedSuperclass()
            .verify();
    }
}
