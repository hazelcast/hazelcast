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

package com.hazelcast.query.impl.predicate;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.ConnectorPredicate;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
        public boolean isSubSet(Predicate predicate) {
            return false;
        }

        @Override
        public Set<QueryableEntry> filter(final QueryContext queryContext) {
            return null;
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
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
    public void testAndIsSubset_whenComplexPredicate() {
        ConnectorPredicate and = new AndPredicate(equal(null, 3),
                and(equal(null, 10), notEqual(null, 15), or(equal(null, 44), notEqual(null, 45)),
                                and(equal(null, 4), equal(null, 5))),
                equal(null, 99));

        assertTrue(and.contains(notEqual(null, 15)));
        assertTrue(and.contains(equal(null, 99)));
        assertTrue(and.contains(and(equal(null, 4), equal(null, 5))));
        assertTrue(and.contains(or(equal(null, 44), notEqual(null, 45))));

        assertFalse(and.contains(notEqual(null, 45)));
        assertFalse(and.contains(and(equal(null, 4), equal(null, 5), equal(null, 6))));
        assertFalse(and.contains(and(equal(null, 1), equal(null, 6))));
        assertFalse(and.contains(notEqual(null, 11)));
    }

    @Test
    public void testIsOrSubset_whenComplexPredicate() {
        Predicate and1 = and(equal(null, 10), notEqual(null, 15), or(equal(null, 44), notEqual(null, 45)),
                and(equal(null, 4), equal(null, 5)));
        ConnectorPredicate or = new OrPredicate(equal(null, 3),
                and1,
                equal(null, 99), or(equal(null, 1), equal(null, 5)));

        assertTrue(or.contains(equal(null, 99)));
        assertTrue(or.contains(equal(null, 99)));
        assertTrue(or.contains(equal(null, 3)));
        assertTrue(or.contains(and1));
        assertTrue(or.contains(or(equal(null, 1), equal(null, 5))));

        assertFalse(or.contains(or(equal(null, 3), notEqual(null, 99))));
        assertFalse(or.contains(notEqual(null, 15)));
        assertFalse(or.contains(or(equal(null, 44), notEqual(null, 45))));
        assertFalse(or.contains(and(equal(null, 4), equal(null, 5))));
        assertFalse(or.contains(notEqual(null, 44)));
        assertFalse(or.contains(notEqual(null, 45)));
    }

    @Test
    public void testEqualPredicateEquals() {
        assertTrue(equal("a", 4).equals(equal("a", 4)));
        assertFalse(equal("a", 3).equals(equal("a", 4)));
        assertFalse(equal("a", 3).equals(equal("b", 4)));
        assertFalse(equal(null, 3).equals(equal("a", 4)));
        assertFalse(equal(null, 3).equals(equal(null, 4)));
        assertFalse(equal(null, 3).equals(equal(null, 4)));
        assertTrue(equal(null, 4).equals(equal(null, 4)));
    }

    @Test
    public void testNotEqualPredicateEquals() {
        assertTrue(notEqual("a", 4).equals(notEqual("a", 4)));
        assertFalse(notEqual("a", 3).equals(notEqual("a", 4)));
        assertFalse(notEqual("a", 3).equals(notEqual("b", 4)));
        assertFalse(notEqual(null, 3).equals(notEqual("a", 4)));
        assertFalse(notEqual(null, 3).equals(notEqual(null, 4)));
        assertFalse(notEqual(null, 3).equals(notEqual(null, 4)));
        assertTrue(notEqual(null, 4).equals(notEqual(null, 4)));
    }

    @Test
    public void testBetweenPredicateEquals() {
        assertTrue(between("a", 4, 6).equals(between("a", 4, 6)));
        assertFalse(between("b", 4, 6).equals(between("a", 4, 6)));
        assertFalse(between("a", 4, 7).equals(between("a", 4, 6)));
        assertFalse(between("a", 4, 6).equals(between("a", 5, 6)));
        assertFalse(between("b", 3, 6).equals(between("a", 5, 5)));
        assertTrue(between(null, 3, 6).equals(between(null, 3, 6)));
        assertFalse(between("a", 5, 5).equals(between(null, 5, 5)));
        assertFalse(between(null, 5, 5).equals(between("a", 5, 5)));
    }

    @Test
    public void testLikePredicateEquals() {
        assertEquals(like("a", "ff%"), like("a", "ff%"));
        assertNotEquals(like("a", "ff%"), like("a", "f%"));
        assertNotEquals(like("a", "ff%"), like("b", "ff%"));
        assertNotEquals(like(null, "ff%"), like("b", "ff%"));
        assertEquals(like(null, "ff%"), like(null, "ff%"));
    }

    @Test
    public void testILikePredicateEquals() {
        assertEquals(ilike("a", "ff%"), ilike("a", "ff%"));
        assertNotEquals(ilike("a", "ff%"), ilike("a", "f%"));
        assertNotEquals(ilike("a", "ff%"), ilike("b", "ff%"));
        assertNotEquals(ilike(null, "ff%"), ilike("b", "ff%"));
        assertEquals(ilike(null, "ff%"), ilike(null, "ff%"));
    }

    @Test
    public void testInPredicateEquals() {
        assertEquals(in("attr", "a", "b"), in("attr", "a", "b"));
        assertEquals(in("attr", "b", "a"), in("attr", "b", "a"));
        assertNotEquals(in("attr", "b", "a", "b"), in("attr", "b", "a"));
        assertEquals(in("attr"), in("attr"));
        assertNotEquals(in("attr", "a"), in("attr", "ab"));
        assertNotEquals(in("attr", "a"), in("attr", "a", "b"));
        assertNotEquals(in("attr", "c", "d"), in("attr", "a", "b"));
    }

    @Test
    public void testInstanceOfPredicateEquals() {
        assertEquals(instanceOf(Long.class), instanceOf(Long.class));
        assertNotEquals(instanceOf(Long.class), instanceOf(Number.class));
        assertNotEquals(instanceOf(Long.class), instanceOf(String.class));
    }

    @Test
    public void testAndPredicateEquals() {
        // todo: null argument
        assertEquals(and(greaterEqual("a", 4), instanceOf(Long.class)), and(greaterEqual("a", 4), instanceOf(Long.class)));
        assertEquals(and(greaterEqual("a", 4), and(instanceOf(Long.class), instanceOf(Object.class))),
                     and(greaterEqual("a", 4), and(instanceOf(Object.class), instanceOf(Long.class))));
        assertEquals(and(greaterEqual("a", 4), or(instanceOf(Long.class), instanceOf(Object.class))),
                and(greaterEqual("a", 4), or(instanceOf(Object.class), instanceOf(Long.class))));
        assertEquals(and(greaterEqual("a", 4), instanceOf(Long.class)),
                and(greaterEqual("a", 4), instanceOf(Long.class)));
    }

    @Test
    public void testOrPredicateEquals() {
        // todo: null argument
        assertEquals(or(greaterEqual("a", 4), instanceOf(Long.class)), or(greaterEqual("a", 4), instanceOf(Long.class)));
        assertEquals(or(greaterEqual("a", 4), or(instanceOf(Long.class), instanceOf(Object.class))),
                     or(greaterEqual("a", 4), or(instanceOf(Object.class), instanceOf(Long.class))));
        assertEquals(or(greaterEqual("a", 4), instanceOf(Long.class)),
                or(greaterEqual("a", 4), instanceOf(Long.class)));
        assertEquals(or(greaterEqual("a", 4), instanceOf(Long.class)),
                or(greaterEqual("a", 4), instanceOf(Long.class)));
    }

    @Test
    public void testSqlPredicateEquals() {
        assertEquals(SqlPredicate.createPredicate("a=5"), SqlPredicate.createPredicate("a=5"));
        assertEquals(SqlPredicate.createPredicate("a=5 and B='test'"), SqlPredicate.createPredicate("a=5 and B='test'"));
        assertEquals(SqlPredicate.createPredicate("a=5 and (B='test' or c=4)"), SqlPredicate.createPredicate("a=5 and (B='test' or c=4)"));
        assertNotEquals(SqlPredicate.createPredicate("a=5 and (B='test' or c=4)"), SqlPredicate.createPredicate("a=5 and (B='test' and c=4)"));
        assertNotEquals(SqlPredicate.createPredicate("a=5"), SqlPredicate.createPredicate("a=4"));
    }

    @Test
    public void testPredicateBuilderEquals() {
        PredicateBuilder pb = new PredicateBuilder();
        EntryObject eo = pb.getEntryObject();
        eo.get("test").equal(4);
        eo.get("test2").equal(5);

        PredicateBuilder pb1 = new PredicateBuilder();
        EntryObject eo1 = pb1.getEntryObject();
        eo1.get("test").equal(4);
        eo1.get("test2").equal(5);

        PredicateBuilder predicateBuilder = new PredicateBuilder();
        predicateBuilder.getEntryObject().get("attr").between(5,6);

        assertEquals(pb1.build(), pb.build());
        assertEquals(pb1.and(eo1.get("rand").equal(5)).build(), pb.and(eo.get("rand").equal(5)).build());
//        assertNotEquals(pb1.build(), pb.and(eo.get("rand").equal(5)).build());
    }

    @Test
    public void testGreaterLessPredicateEquals() {
        assertEquals(greaterEqual("a", 4), greaterEqual("a", 4));
        assertNotEquals(greaterEqual("a", 5), greaterEqual("a", 4));
        assertNotEquals(greaterEqual("b", 4), greaterEqual("a", 4));
        assertNotEquals(greaterEqual(null, 4), greaterEqual("a", 4));
        assertEquals(greaterEqual(null, 4), greaterEqual(null, 4));

        assertEquals(greaterThan("a", 4), greaterThan("a", 4));
        assertNotEquals(greaterThan("a", 5), greaterThan("a", 4));
        assertNotEquals(greaterThan("b", 4), greaterThan("a", 4));
        assertNotEquals(greaterThan(null, 4), greaterThan("a", 4));
        assertEquals(greaterThan(null, 4), greaterThan(null, 4));

        assertEquals(lessEqual("a", 4), lessEqual("a", 4));
        assertNotEquals(lessEqual("a", 5), lessEqual("a", 4));
        assertNotEquals(lessEqual("b", 4), lessEqual("a", 4));
        assertNotEquals(lessEqual(null, 4), lessEqual("a", 4));
        assertEquals(lessEqual(null, 4), lessEqual(null, 4));

        assertEquals(lessThan("a", 4), lessThan("a", 4));
        assertNotEquals(lessThan("a", 5), lessThan("a", 4));
        assertNotEquals(lessThan("b", 4), lessThan("a", 4));
        assertNotEquals(lessThan(null, 4), lessThan("a", 4));
        assertEquals(lessThan(null, 4), lessThan(null, 4));
    }

    @Test
    public void testPredicateIn() {
        assertTrue(between(null, 15, 20).isSubSet(between(null, 12, 20)));
        assertTrue(in("gg", 4, 20, 5).isSubSet(in("gg", 4, 5, 20, 89)));
        assertTrue(greaterThan(null, 15).isSubSet(greaterThan(null, 10)));
        assertTrue(greaterEqual(null, 99).isSubSet(greaterEqual(null, 99)));
        assertTrue(lessThan(null, 97).isSubSet(lessThan(null, 98)));
        assertTrue(lessEqual(null, 97).isSubSet(lessEqual(null, 100)));
    }

    @Test
    public void testPredicateSqlContains() {
        Predicate sqlPredicate = SqlPredicate.createPredicate("(((disabled_for_upload != true AND media_id != -1) AND qc_approval_media_state != 22) AND proxy_creation_state=14) AND content_approval_media_state != 18");
        Predicate sqlPredicate1 = SqlPredicate.createPredicate("qc_approval_media_state != 22");
        assertTrue(((ConnectorPredicate) sqlPredicate).contains(sqlPredicate1));
    }

    @Test
    public void testPredicateBuilderSubset() {
        EntryObject eo = new PredicateBuilder().getEntryObject();
        Predicate p0 = eo
                .isNot("disabled_for_upload")
                .and(eo.get("media_id").notEqual(-1))
                .and(eo.get("qc_approval_media_state").notEqual(22))
                .and(eo.get("proxy_creation_state").equal(14))
                .and(eo.get("content_approval_media_state").notEqual(18))
                .and(eo.isNot("inactive")).build();

        EntryObject eo1 = new PredicateBuilder().getEntryObject();
        Predicate p1 = eo1
                .get("quality_check_state").equal(12)
                .and(eo1.get("clock_approval_media_state").notEqual(42))
                .and(eo1.get("class_approval_media_state").notEqual(72)).build();

        AndPredicate andPredicate = new AndPredicate(p1, p0);
        assertTrue(andPredicate.contains(p1));
        assertEquals(andPredicate.subtract(p1), p0);
    }

    @Test
    public void tesConnectorPredicateEqual_whenMixedOrder() {
        Predicate and0 = and(equal(null, 3),
                and(equal(null, 10), notEqual(null, 15), equal(null, 4), equal(null, 5)),
                equal(null, 99));
        Predicate and1 = and(equal(null, 99),
                and(equal(null, 10), equal(null, 4),  notEqual(null, 15), equal(null, 5)),
                equal(null, 3));

        assertTrue(and0.equals(and1));
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
        Predicate predicate = e2.greaterEqual(29).and(e2.lessEqual(36)).build();
        assertTrue(predicate.apply(createEntry("1", value)));
        e = new PredicateBuilder().getEntryObject();
        assertTrue(e.get("id").equal(12).build().apply(createEntry("1", value)));
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
        in("", null);
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
