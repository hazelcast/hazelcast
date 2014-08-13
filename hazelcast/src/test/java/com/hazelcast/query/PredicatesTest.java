/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.ReflectionHelper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.instance.TestUtil.toData;
import static com.hazelcast.query.Predicates.AndPredicate;
import static com.hazelcast.query.Predicates.EqualPredicate;
import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.between;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.ilike;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.Predicates.instanceOf;
import static com.hazelcast.query.Predicates.lessEqual;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.like;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.query.Predicates.or;
import static com.hazelcast.query.Predicates.regex;
import static com.hazelcast.query.SampleObjects.Employee;
import static com.hazelcast.query.SampleObjects.Value;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Map.Entry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PredicatesTest extends HazelcastTestSupport {

    @Test
    public void testAndPredicate_whenFirstIndexAwarePredicateIsNotIndexed() throws Exception {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Object, Object> map = instance.getMap("map");
        map.addIndex("name", false);
        String name = randomString();
        map.put("key", new Value(name));

        final ShouldExecuteOncePredicate indexAwareNotIndexedPredicate = new ShouldExecuteOncePredicate();
        final EqualPredicate equalPredicate = new EqualPredicate("name", name);
        final AndPredicate andPredicate = new AndPredicate(indexAwareNotIndexedPredicate, equalPredicate);
        map.values(andPredicate);
    }

    static class ShouldExecuteOncePredicate implements IndexAwarePredicate {

        boolean executed = false;

        @Override
        public boolean apply(Map.Entry mapEntry) {
            if (!executed) {
                executed = true;
                return true;
            }
            throw new RuntimeException();
        }

        @Override
        public Set<QueryableEntry> filter(final QueryContext queryContext) {
            return null;
        }

        @Override
        public boolean isIndexed(final QueryContext queryContext) {
            return false;
        }
    }

    @Test
    public void testEqual() {
        assertPredicateTrue(equal(null, "value"), "value");
        assertPredicateFalse(equal(null, "value1"), "value");
        assertPredicateTrue(equal(null, TRUE), true);
        assertPredicateTrue(equal(null, true), TRUE);
        assertPredicateFalse(equal(null, true), FALSE);
        assertPredicateFalse(equal(null, new BigDecimal("1.23E3")), new BigDecimal("1.23E2"));
        assertPredicateTrue(equal(null, new BigDecimal("1.23E3")), new BigDecimal("1.23E3"));
        assertPredicateFalse(equal(null, 15.22), 15.23);
        assertPredicateTrue(equal(null, 15.22), 15.22);
        assertPredicateFalse(equal(null, 16), 15);
    }


    @Test
    public void testAnd() {
        final Predicate and1 = and(greaterThan(null, 4), lessThan(null, 6));
        assertPredicateTrue(and1, 5);
        final Predicate and2 = and(greaterThan(null, 5), lessThan(null, 6));
        assertPredicateFalse(and2, 4);
        final Predicate and3 = and(greaterThan(null, 4), lessThan(null, 6), equal(null, 5));
        assertPredicateTrue(and3, 5);
        final Predicate and4 = and(greaterThan(null, 3), lessThan(null, 6), equal(null, 4));
        assertPredicateFalse(and4, 5);
    }

    @Test
    public void testOr() {
        final Predicate or1 = or(equal(null, 3), equal(null, 4), equal(null, 5));
        assertPredicateTrue(or1, 4);
        assertPredicateFalse(or1, 6);
    }

    @Test
    public void testGreaterEqual() {
        assertPredicateTrue(greaterEqual(null, 5), 5);
    }

    @Test
    public void testLessThan() {
        assertPredicateTrue(lessThan(null, 7), 6);
        assertPredicateFalse(lessThan(null, 3), 4);
        assertPredicateFalse(lessThan(null, 4), 4);
        assertPredicateTrue(lessThan(null, "tc"), "bz");
        assertPredicateFalse(lessThan(null, "gx"), "h0");
    }

    @Test
    public void testGreaterThan() {
        assertPredicateTrue(greaterThan(null, 5), 6);
        assertPredicateFalse(greaterThan(null, 5), 4);
        assertPredicateFalse(greaterThan(null, 5), 5);
        assertPredicateTrue(greaterThan(null, "aa"), "xa");
        assertPredicateFalse(greaterThan(null, "da"), "cz");
        assertPredicateTrue(greaterThan(null, new BigDecimal("1.23E2")), new BigDecimal("1.23E3"));
    }

    @Test
    public void testLessEqual() {
        assertPredicateTrue(lessEqual(null, 4), 4);
    }

    @Test
    public void testPredicatesAgainstANullField() {
        assertFalse_withNullEntry(lessEqual("nullField", 1));

        assertFalse_withNullEntry(in("nullField", 1));
        assertFalse_withNullEntry(lessThan("nullField", 1));
        assertFalse_withNullEntry(greaterEqual("nullField", 1));
        assertFalse_withNullEntry(greaterThan("nullField", 1));
        assertFalse_withNullEntry(equal("nullField", 1));
        assertFalse_withNullEntry(notEqual("nullField", null));
        assertFalse_withNullEntry(between("nullField", 1, 1));
        assertTrue_withNullEntry(like("nullField", null));
        assertTrue_withNullEntry(ilike("nullField", null));
        assertTrue_withNullEntry(regex("nullField", null));
    }

    @Test
    public void testBetween() {
        assertPredicateTrue(between(null, 4, 6), 5);
        assertPredicateTrue(between(null, 5, 6), 5);
        assertPredicateTrue(between(null, "abc", "xyz"), "prs");
        assertPredicateFalse(between(null, "klmn", "xyz"), "efgh");
        assertPredicateFalse(between(null, 6, 7), 5);
    }

    @Test
    public void testIn() {
        assertPredicateTrue(in(null, 4, 7, 8, 5), 5);
        assertPredicateTrue(in(null, 5, 7, 8), 5);
        assertPredicateFalse(in(null, 6, 7, 8), 5);
        assertPredicateFalse(in(null, 6, 7, 8), 9);
    }

    @Test
    public void testLike() {
        assertPredicateTrue(like(null, "J%"), "Java");
        assertPredicateTrue(like(null, "Ja%"), "Java");
        assertPredicateTrue(like(null, "J_v_"), "Java");
        assertPredicateTrue(like(null, "_av_"), "Java");
        assertPredicateTrue(like(null, "_a__"), "Java");
        assertPredicateTrue(like(null, "J%v_"), "Java");
        assertPredicateTrue(like(null, "J%_"), "Java");
        assertPredicateFalse(like(null, "java"), "Java");
        assertPredicateFalse(like(null, "j%"), "Java");
        assertPredicateFalse(like(null, "J_a"), "Java");
        assertPredicateFalse(like(null, "J_ava"), "Java");
        assertPredicateFalse(like(null, "J_a_a"), "Java");
        assertPredicateFalse(like(null, "J_av__"), "Java");
        assertPredicateFalse(like(null, "J_Va"), "Java");
        assertPredicateTrue(like(null, "Java World"), "Java World");
        assertPredicateTrue(like(null, "Java%ld"), "Java World");
        assertPredicateTrue(like(null, "%World"), "Java World");
        assertPredicateTrue(like(null, "Java_World"), "Java World");

        assertPredicateTrue(like(null, "J.-*.*\\%"), "J.-*.*%");
        assertPredicateTrue(like(null, "J\\_"), "J_");
        assertPredicateTrue(like(null, "J%"), "Java");

    }

    @Test
    public void testILike() {
        assertPredicateFalse(like(null, "JavaWorld"), "Java World");
        assertPredicateTrue(ilike(null, "Java_World"), "java World");
        assertPredicateTrue(ilike(null, "java%ld"), "Java World");
        assertPredicateTrue(ilike(null, "%world"), "Java World");
        assertPredicateFalse(ilike(null, "Java_World"), "gava World");
    }

    @Test
    public void testIsInstanceOf() {
        assertTrue(instanceOf(Long.class).apply(new DummyEntry(1L)));
        assertFalse(instanceOf(Long.class).apply(new DummyEntry("Java")));
        assertTrue(instanceOf(Number.class).apply(new DummyEntry(4)));
    }

    @Test
    public void testCriteriaAPI() {
        Object value = new Employee(12, "abc-123-xvz", 34, true, 10D);
        EntryObject e = new PredicateBuilder().getEntryObject();
        EntryObject e2 = e.get("age");
        Predicate predicate = e2.greaterEqual(29).and(e2.lessEqual(36));
        assertTrue(predicate.apply(createEntry("1", value)));
        e = new PredicateBuilder().getEntryObject();
        assertTrue(e.get("id").equal(12).apply(createEntry("1", value)));
    }

    @Test(expected = NullPointerException.class)
    public void testBetweenNull() {
        Predicates.between("", null, null);
    }

    @Test(expected = NullPointerException.class)
    public void testLessThanNull() {
        Predicates.lessThan("", null);
    }

    @Test(expected = NullPointerException.class)
    public void testLessEqualNull() {
        Predicates.lessEqual("", null);
    }

    @Test(expected = NullPointerException.class)
    public void testGreaterThanNull() {
        Predicates.greaterThan("", null);
    }

    @Test(expected = NullPointerException.class)
    public void testGreaterEqualNull() {
        Predicates.greaterEqual("", null);
    }

    @Test(expected = NullPointerException.class)
    public void testInNullWithNullArray() {
        Predicates.in("", null);
    }

    private class DummyEntry extends QueryEntry {

        DummyEntry(Comparable attribute) {
            super(null, toData("1"), "1", attribute);
        }

        @Override
        public Comparable getAttribute(String attributeName) throws QueryException {
            return (Comparable) getValue();
        }

        @Override
        public AttributeType getAttributeType(String attributeName) {
            return ReflectionHelper.getAttributeType(getValue().getClass());
        }
    }

    private class NullDummyEntry implements QueryableEntry {

        private Integer nullField;

        private NullDummyEntry() {
        }

        public Integer getNullField() {
            return nullField;
        }

        public void setNullField(Integer nullField) {
            this.nullField = nullField;
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public Object setValue(Object value) {
            return null;
        }

        @Override
        public Object getKey() {
            return 1;
        }

        @Override
        public Comparable getAttribute(String attributeName) throws QueryException {
            return null;
        }

        @Override
        public AttributeType getAttributeType(String attributeName) {
            return AttributeType.INTEGER;
        }

        @Override
        public Data getKeyData() {
            return null;
        }

        @Override
        public Data getValueData() {
            return null;
        }

        @Override
        public Data getIndexKey() {
            return null;
        }

    }

    private static Entry createEntry(final Object key, final Object value) {
        return new QueryEntry(null, toData(key), key, value);
    }

    private void assertPredicateTrue(Predicate p, Comparable comparable) {
        assertTrue(p.apply(new DummyEntry(comparable)));
    }

    private void assertPredicateFalse(Predicate p, Comparable comparable) {
        assertFalse(p.apply(new DummyEntry(comparable)));
    }

    private void assertTrue_withNullEntry(Predicate p) {
        assertTrue(p.apply(new NullDummyEntry()));
    }

    private void assertFalse_withNullEntry(Predicate p) {
        assertFalse(p.apply(new NullDummyEntry()));
    }
}
