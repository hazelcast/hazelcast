/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.ILikePredicate;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.TestUtil.toData;
import static com.hazelcast.jet.GenericPredicates.alwaysFalse;
import static com.hazelcast.jet.GenericPredicates.alwaysTrue;
import static com.hazelcast.jet.GenericPredicates.and;
import static com.hazelcast.jet.GenericPredicates.between;
import static com.hazelcast.jet.GenericPredicates.equal;
import static com.hazelcast.jet.GenericPredicates.greaterEqual;
import static com.hazelcast.jet.GenericPredicates.greaterThan;
import static com.hazelcast.jet.GenericPredicates.ilike;
import static com.hazelcast.jet.GenericPredicates.in;
import static com.hazelcast.jet.GenericPredicates.instanceOf;
import static com.hazelcast.jet.GenericPredicates.lessEqual;
import static com.hazelcast.jet.GenericPredicates.lessThan;
import static com.hazelcast.jet.GenericPredicates.like;
import static com.hazelcast.jet.GenericPredicates.not;
import static com.hazelcast.jet.GenericPredicates.notEqual;
import static com.hazelcast.jet.GenericPredicates.or;
import static com.hazelcast.jet.GenericPredicates.regex;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * This is a stripped-down version of {@code com.hazelcast.query.impl.predicates.PredicatesTest}.
 */
@RunWith(HazelcastSerialClassRunner.class)
public class GenericPredicatesTest extends HazelcastTestSupport {

    private static final String ATTRIBUTE = "DUMMY_ATTRIBUTE_IGNORED";

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testAlwaysTrue() {
        assertPredicateTrue(alwaysTrue(), "value");
    }

    @Test
    public void testAlwaysFalse() {
        assertPredicateFalse(alwaysFalse(), "value");
    }

    @Test
    public void testNot() {
        assertPredicateFalse(not(alwaysTrue()), "value");
    }

    @Test
    public void testRegex() {
        assertPredicateTrue(regex(ATTRIBUTE, "\\w+"), "value");
    }

    @Test
    public void testEqual() {
        assertPredicateTrue(equal(ATTRIBUTE, "value"), "value");
        assertPredicateFalse(equal(ATTRIBUTE, "value1"), "value");
    }

    @Test
    public void testNotEqual() {
        assertPredicateTrue(notEqual(ATTRIBUTE, "value"), "value1");
        assertPredicateFalse(notEqual(ATTRIBUTE, "value"), "value");
    }

    @Test
    public void testAnd() {
        final Predicate and1 = and(greaterThan(ATTRIBUTE, 4), lessThan(ATTRIBUTE, 6));
        assertPredicateTrue(and1, 5);
        final Predicate and2 = and(greaterThan(ATTRIBUTE, 5), lessThan(ATTRIBUTE, 6));
        assertPredicateFalse(and2, 4);
    }

    @Test
    public void testOr() {
        final Predicate or1 = or(equal(ATTRIBUTE, 3), equal(ATTRIBUTE, 4), equal(ATTRIBUTE, 5));
        assertPredicateTrue(or1, 4);
        assertPredicateFalse(or1, 6);
    }

    @Test
    public void testGreaterEqual() {
        assertPredicateTrue(greaterEqual(ATTRIBUTE, 5), 5);
    }

    @Test
    public void testLessThan() {
        assertPredicateTrue(lessThan(ATTRIBUTE, 7), 6);
        assertPredicateFalse(lessThan(ATTRIBUTE, 3), 4);
    }

    @Test
    public void testGreaterThan() {
        assertPredicateTrue(greaterThan(ATTRIBUTE, 5), 6);
        assertPredicateFalse(greaterThan(ATTRIBUTE, 5), 4);
    }

    @Test
    public void testLessEqual() {
        assertPredicateTrue(lessEqual(ATTRIBUTE, 4), 4);
    }

    @Test
    public void testBetween() {
        assertPredicateTrue(between(ATTRIBUTE, 4, 6), 5);
        assertPredicateFalse(between(ATTRIBUTE, "klmn", "xyz"), "efgh");
    }

    @Test
    public void testIn() {
        assertPredicateTrue(in(ATTRIBUTE, 4, 7, 8, 5), 5);
        assertPredicateFalse(in(ATTRIBUTE, 6, 7, 8), 5);
    }

    @Test
    public void testLike() {
        assertPredicateTrue(like(ATTRIBUTE, "J%"), "Java");
    }

    @Test
    public void testILike() {
        assertPredicateTrue(ilike(ATTRIBUTE, "Java_World"), "java World");
    }

    @Test
    public void testILike_Id() {
        ILikePredicate predicate = (ILikePredicate) ilike(ATTRIBUTE, "Java_World");

        assertThat(predicate.getId(), allOf(equalTo(6), equalTo(PredicateDataSerializerHook.ILIKE_PREDICATE)));
    }

    @Test
    public void testIsInstanceOf() {
        assertTrue(instanceOf(Long.class).apply(new DummyEntry(1L)));
        assertFalse(instanceOf(Long.class).apply(new DummyEntry("Java")));
    }

    private class DummyEntry extends QueryEntry {

        DummyEntry(Comparable attribute) {
            super(ss, toData("1"), attribute, Extractors.newBuilder(ss).build());
        }

        @Override
        public Comparable getAttributeValue(String attributeName) throws QueryException {
            return (Comparable) getValue();
        }
    }

    private void assertPredicateTrue(Predicate p, Comparable comparable) {
        assertTrue(p.apply(new DummyEntry(comparable)));
    }

    private void assertPredicateFalse(Predicate p, Comparable comparable) {
        assertFalse(p.apply(new DummyEntry(comparable)));
    }
}
