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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.impl.predicates.PredicateTestUtils.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GreaterLessPredicateTest {

    @Test
    public void negate_whenEqualsTrueAndLessTrue_thenReturnNewInstanceWithEqualsFalseAndLessFalse() {
        String attribute = "attribute";
        Comparable value = 1;

        GreaterLessPredicate original = new GreaterLessPredicate(attribute, value, true, true);
        GreaterLessPredicate negate = (GreaterLessPredicate) original.negate();

        assertThat(negate).isNotSameAs(original);
        assertThat(negate.attributeName).isEqualTo(attribute);
        assertThat(negate.equal).isFalse();
        assertThat(negate.less).isFalse();
    }

    @Test
    public void negate_whenEqualsFalseAndLessFalse_thenReturnNewInstanceWithEqualsTrueAndLessTrue() {
        String attribute = "attribute";
        Comparable value = 1;

        GreaterLessPredicate original = new GreaterLessPredicate(attribute, value, false, false);
        GreaterLessPredicate negate = (GreaterLessPredicate) original.negate();

        assertThat(negate).isNotSameAs(original);
        assertThat(negate.attributeName).isEqualTo(attribute);
        assertThat(negate.equal).isTrue();
        assertThat(negate.less).isTrue();
    }

    @Test
    public void negate_whenEqualsTrueAndLessFalse_thenReturnNewInstanceWithEqualsFalseAndLessTrue() {
        String attribute = "attribute";
        Comparable value = 1;

        GreaterLessPredicate original = new GreaterLessPredicate(attribute, value, true, false);
        GreaterLessPredicate negate = (GreaterLessPredicate) original.negate();

        assertThat(negate).isNotSameAs(original);
        assertThat(negate.attributeName).isEqualTo(attribute);
        assertThat(negate.equal).isFalse();
        assertThat(negate.less).isTrue();
    }

    @Test
    public void negate_whenEqualsFalseAndLessTrue_thenReturnNewInstanceWithEqualsTrueAndLessFalse() {
        String attribute = "attribute";
        Comparable value = 1;

        GreaterLessPredicate original = new GreaterLessPredicate(attribute, value, false, true);
        GreaterLessPredicate negate = (GreaterLessPredicate) original.negate();

        assertThat(negate).isNotSameAs(original);
        assertThat(negate.attributeName).isEqualTo(attribute);
        assertThat(negate.equal).isTrue();
        assertThat(negate.less).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    // should be fixed in 4.0; See: https://github.com/hazelcast/hazelcast/issues/6188
    public void equal_zeroMinusZero() {
        final boolean equal = true;
        final boolean less = false;

        // in GreaterLessPredicate predicate
        assertFalse(new GreaterLessPredicate("this", 0.0, equal, less).apply(entry(-0.0)));
        assertFalse(new GreaterLessPredicate("this", 0.0d, equal, less).apply(entry(-0.0d)));
        assertFalse(new GreaterLessPredicate("this", 0.0, equal, less).apply(entry(-0.0d)));
        assertFalse(new GreaterLessPredicate("this", 0.0d, equal, less).apply(entry(-0.0)));

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
        final boolean equal = true;
        final boolean less = false;

        // in GreaterLessPredicate
        assertTrue(new GreaterLessPredicate("this", Double.NaN, equal, less).apply(entry(Double.NaN)));
        assertTrue(new GreaterLessPredicate("this", Float.NaN, equal, less).apply(entry(Float.NaN)));
        assertTrue(new GreaterLessPredicate("this", Double.NaN, equal, less).apply(entry(-Double.NaN)));
        assertTrue(new GreaterLessPredicate("this", Float.NaN, equal, less).apply(entry(-Float.NaN)));
        assertTrue(new GreaterLessPredicate("this", Double.NaN, equal, less).apply(entry(-Float.NaN)));
        assertTrue(new GreaterLessPredicate("this", Float.NaN, equal, less).apply(entry(-Double.NaN)));

        // whereas in Java
        assertFalse(Double.NaN == Double.NaN);
        assertFalse(Float.NaN == Float.NaN);
        assertFalse(Double.NaN == -Double.NaN);
        assertFalse(Float.NaN == -Float.NaN);
        assertFalse(Double.NaN == -Float.NaN);
        assertFalse(Float.NaN == -Double.NaN);
    }

    @Test
    @SuppressWarnings("unchecked")
    // should be fixed in 4.0; See: https://github.com/hazelcast/hazelcast/issues/6188
    public void greaterThan() {
        final boolean equal = true;

        // in GreaterLessPredicate
        assertTrue(new GreaterLessPredicate("this", 100.0, equal, false).apply(entry(Double.NaN)));
        assertTrue(new GreaterLessPredicate("this", 100.0d, equal, false).apply(entry(Double.NaN)));
        assertTrue(new GreaterLessPredicate("this", 100.0, equal, false).apply(entry(Float.NaN)));
        assertTrue(new GreaterLessPredicate("this", 100.0d, equal, false).apply(entry(Float.NaN)));

        assertTrue(new GreaterLessPredicate("this", 100.0, equal, false).apply(entry(-Double.NaN)));
        assertTrue(new GreaterLessPredicate("this", 100.0d, equal, false).apply(entry(-Double.NaN)));
        assertTrue(new GreaterLessPredicate("this", 100.0, equal, false).apply(entry(-Float.NaN)));
        assertTrue(new GreaterLessPredicate("this", 100.0d, equal, false).apply(entry(-Float.NaN)));

        assertFalse(new GreaterLessPredicate("this", -100.0, equal, true).apply(entry(-Double.NaN)));
        assertFalse(new GreaterLessPredicate("this", -100.0d, equal, true).apply(entry(-Double.NaN)));
        assertFalse(new GreaterLessPredicate("this", -100.0, equal, true).apply(entry(-Float.NaN)));
        assertFalse(new GreaterLessPredicate("this", -100.0d, equal, true).apply(entry(-Float.NaN)));

        // whereas in Java
        assertFalse(Double.NaN > 100.0);
        assertFalse(Double.NaN > 100.0d);
        assertFalse(Float.NaN > 100.0);
        assertFalse(Float.NaN > 100.0d);

        assertFalse(-Double.NaN > 100.0);
        assertFalse(-Double.NaN > 100.0d);
        assertFalse(-Float.NaN > 100.0);
        assertFalse(-Float.NaN > 100.0d);

        assertFalse(-Double.NaN < -100.0);
        assertFalse(-Double.NaN < -100.0d);
        assertFalse(-Float.NaN < -100.0);
        assertFalse(-Float.NaN < -100.0d);
    }

    @Test
    public void testEqualsAndHashCode() {
        EqualsVerifier.forClass(GreaterLessPredicate.class)
            .suppress(Warning.NONFINAL_FIELDS, Warning.STRICT_INHERITANCE)
            .withRedefinedSuperclass()
            .verify();
    }

}
