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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.DateHelperTest;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.ReflectionHelper;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import static com.hazelcast.instance.TestUtil.toData;
import static com.hazelcast.query.Predicates.greaterThan;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.SampleObjects.Employee;
import static com.hazelcast.query.SampleObjects.State;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PredicatesTest {

    @Test
    public void testSql() {
        Employee value = new Employee("abc-123-xvz", 34, true, 10D);
        value.setState(State.STATE2);
        Employee nullNameValue = new Employee(null, 34, true, 10D);
        assertTrue(new SqlPredicate("state == TestUtil.State.STATE2").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("state == " + State.STATE2).apply(createEntry("1", value)));
        assertFalse(new SqlPredicate("state == TestUtil.State.STATE1").apply(createEntry("1", value)));
        assertFalse(new SqlPredicate("state == TestUtil.State.STATE1").apply(createEntry("1", nullNameValue)));
        assertTrue(new SqlPredicate("createDate >= '" + new SimpleDateFormat(DateHelperTest.DATE_FORMAT, Locale.US).format(new Date(0)) + "'").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("sqlDate >= '" + new java.sql.Date(0) + "'").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("date >= '" + new Timestamp(0) + "'").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("bigDecimal > '" + new BigDecimal("1.23E2") + "'").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("bigDecimal >= '" + new BigDecimal("1.23E3") + "'").apply(createEntry("1", value)));
        assertFalse(new SqlPredicate("bigDecimal = '" + new BigDecimal("1.23") + "'").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("bigDecimal = '1.23E3'").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("bigDecimal = 1.23E3").apply(createEntry("1", value)));
        assertFalse(new SqlPredicate("bigDecimal = 1.23").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("state == NULL").apply(createEntry("1", nullNameValue)));
        assertFalse(new SqlPredicate("name = 'null'").apply(createEntry("1", nullNameValue)));
        assertTrue(new SqlPredicate("name = null").apply(createEntry("1", nullNameValue)));
        assertTrue(new SqlPredicate("name = NULL").apply(createEntry("1", nullNameValue)));
        assertTrue(new SqlPredicate("name != null").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("name != NULL").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("(age >= " + 20 + ") AND (age <= " + 40 + ")").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("(age >= " + 20 + ") AND (age <= " + 34 + ")").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("(age >= " + 34 + ") AND (age <= " + 35 + ")").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("age IN (" + 34 + ", " + 35 + ")").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate(" (name LIKE 'abc-%') AND (age <= " + 40 + ")").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate(" (name REGEX 'abc-.*') AND (age <= " + 40 + ")").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("age = -33").apply(createEntry("1", new Employee("abc-123-xvz", -33, true, 10D))));
        assertFalse(new SqlPredicate("age = 33").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("age = 34").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("age > 5").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("salary > 5").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("salary > 5 and salary < 11").apply(createEntry("1", value)));
        assertFalse(new SqlPredicate("salary > 15 or salary < 10").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("salary between 9.99 and 10.01").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("salary between 5 and 15").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("name='abc-123-xvz'").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate("name='abc 123-xvz'").apply(createEntry("1", new Employee("abc 123-xvz", 34, true, 10D))));
        assertTrue(new SqlPredicate("name='abc 123-xvz+(123)'").apply(createEntry("1", new Employee("abc 123-xvz+(123)", 34, true, 10D))));
        assertFalse(new SqlPredicate("name='abc 123-xvz+(123)'").apply(createEntry("1", new Employee("abc123-xvz+(123)", 34, true, 10D))));
        assertTrue(new SqlPredicate("name LIKE 'abc-%'").apply(createEntry("1", new Employee("abc-123", 34, true, 10D))));
        assertTrue(new SqlPredicate("name REGEX '^\\w{3}-\\d{3}-\\w{3}$'").apply(createEntry("1", value)));
        assertFalse(new SqlPredicate("name REGEX '^[^\\w]{3}-\\d{3}-\\w{3}$'").apply(createEntry("1", value)));
        assertTrue(new SqlPredicate(" (name ILIKE 'ABC-%') AND (age <= " + 40 + ")").apply(createEntry("1", value)));
    }

    @Test
    public void testEqual() {
        assertTrue(Predicates.equal(null, "value").apply(new DummyEntry("value")));
        assertFalse(Predicates.equal(null, "value1").apply(new DummyEntry("value")));
        assertTrue(Predicates.equal(null, TRUE).apply(new DummyEntry(true)));
        assertTrue(Predicates.equal(null, true).apply(new DummyEntry(TRUE)));
        assertFalse(Predicates.equal(null, true).apply(new DummyEntry(FALSE)));
        assertFalse(Predicates.equal(null, new BigDecimal("1.23E3")).apply(new DummyEntry(new BigDecimal("1.23E2"))));
        assertTrue(Predicates.equal(null, new BigDecimal("1.23E3")).apply(new DummyEntry(new BigDecimal("1.23E3"))));
        assertFalse(Predicates.equal(null, 15.22).apply(new DummyEntry(15.23)));
        assertTrue(Predicates.equal(null, 15.22).apply(new DummyEntry(15.22)));
        assertFalse(Predicates.equal(null, 16).apply(new DummyEntry(15)));
    }


    @Test
    public void testAnd() {
        assertTrue(Predicates.and(greaterThan(null, 4), lessThan(null, 6)).apply(new DummyEntry(5)));
        assertFalse(Predicates.and(greaterThan(null, 5), lessThan(null, 6)).apply(new DummyEntry(4)));
    }

    @Test
    public void testGreaterEqual() {
        assertTrue(Predicates.greaterEqual(null, 5).apply(new DummyEntry(5)));
    }

    @Test
    public void testLessThan() {
        assertTrue(lessThan(null, 7).apply(new DummyEntry(6)));
        assertFalse(lessThan(null, 3).apply(new DummyEntry(4)));
        assertFalse(lessThan(null, 4).apply(new DummyEntry(4)));
        assertTrue(lessThan(null, "tc").apply(new DummyEntry("bz")));
        assertFalse(lessThan(null, "gx").apply(new DummyEntry("h0")));
    }

    @Test
    public void testGreaterThan() {
        assertTrue(greaterThan(null, 5).apply(new DummyEntry(6)));
        assertFalse(greaterThan(null, 5).apply(new DummyEntry(4)));
        assertFalse(greaterThan(null, 5).apply(new DummyEntry(5)));
        assertTrue(greaterThan(null, "aa").apply(new DummyEntry("xa")));
        assertFalse(greaterThan(null, "da").apply(new DummyEntry("cz")));
        assertTrue(greaterThan(null, new BigDecimal("1.23E2")).apply(new DummyEntry(new BigDecimal("1.23E3"))));
    }

    @Test
    public void testLessEqual() {
        assertTrue(Predicates.lessEqual(null, 4).apply(new DummyEntry(4)));
    }

    @Test
    public void testPredicatesAgainstANullField() {
        assertFalse(Predicates.lessEqual("nullField", 1).apply(new NullDummyEntry()));
        assertFalse(Predicates.in("nullField", 1).apply(new NullDummyEntry()));
        assertFalse(Predicates.lessThan("nullField", 1).apply(new NullDummyEntry()));
        assertFalse(Predicates.greaterEqual("nullField", 1).apply(new NullDummyEntry()));
        assertFalse(Predicates.greaterThan("nullField", 1).apply(new NullDummyEntry()));
        assertFalse(Predicates.equal("nullField", 1).apply(new NullDummyEntry()));
        assertFalse(Predicates.notEqual("nullField", null).apply(new NullDummyEntry()));
        assertFalse(Predicates.between("nullField", 1, 1).apply(new NullDummyEntry()));
        assertTrue(Predicates.like("nullField", null).apply(new NullDummyEntry()));
        assertTrue(Predicates.ilike("nullField", null).apply(new NullDummyEntry()));
        assertTrue(Predicates.regex("nullField", null).apply(new NullDummyEntry()));
    }

    @Test
    public void testBetween() {
        assertTrue(Predicates.between(null, 4, 6).apply(new DummyEntry(5)));
        assertTrue(Predicates.between(null, 5, 6).apply(new DummyEntry(5)));
        assertTrue(Predicates.between(null, "abc", "xyz").apply(new DummyEntry("prs")));
        assertFalse(Predicates.between(null, "klmn", "xyz").apply(new DummyEntry("efgh")));
        assertFalse(Predicates.between(null, 6, 7).apply(new DummyEntry(5)));
    }

    @Test
    public void testIn() {
        assertTrue(Predicates.in(null, 4, 7, 8, 5).apply(new DummyEntry(5)));
        assertTrue(Predicates.in(null, 5, 7, 8).apply(new DummyEntry(5)));
        assertFalse(Predicates.in(null, 6, 7, 8).apply(new DummyEntry(5)));
        assertFalse(Predicates.in(null, 6, 7, 8).apply(new DummyEntry(9)));
    }

    @Test
    public void testLike() {
        assertTrue(Predicates.like(null, "J%").apply(new DummyEntry("Java")));
        assertTrue(Predicates.like(null, "Ja%").apply(new DummyEntry("Java")));
        assertTrue(Predicates.like(null, "J_v_").apply(new DummyEntry("Java")));
        assertTrue(Predicates.like(null, "_av_").apply(new DummyEntry("Java")));
        assertTrue(Predicates.like(null, "_a__").apply(new DummyEntry("Java")));
        assertTrue(Predicates.like(null, "J%v_").apply(new DummyEntry("Java")));
        assertTrue(Predicates.like(null, "J%_").apply(new DummyEntry("Java")));
        assertFalse(Predicates.like(null, "java").apply(new DummyEntry("Java")));
        assertFalse(Predicates.like(null, "j%").apply(new DummyEntry("Java")));
        assertFalse(Predicates.like(null, "J_a").apply(new DummyEntry("Java")));
        assertFalse(Predicates.like(null, "J_ava").apply(new DummyEntry("Java")));
        assertFalse(Predicates.like(null, "J_a_a").apply(new DummyEntry("Java")));
        assertFalse(Predicates.like(null, "J_av__").apply(new DummyEntry("Java")));
        assertFalse(Predicates.like(null, "J_Va").apply(new DummyEntry("Java")));
        assertTrue(Predicates.like(null, "Java World").apply(new DummyEntry("Java World")));
        assertTrue(Predicates.like(null, "Java%ld").apply(new DummyEntry("Java World")));
        assertTrue(Predicates.like(null, "%World").apply(new DummyEntry("Java World")));
        assertTrue(Predicates.like(null, "Java_World").apply(new DummyEntry("Java World")));

    }

    @Test
    public void testILike() {
        assertFalse(Predicates.like(null, "JavaWorld").apply(new DummyEntry("Java World")));
        assertTrue(Predicates.ilike(null, "Java_World").apply(new DummyEntry("java World")));
        assertTrue(Predicates.ilike(null, "java%ld").apply(new DummyEntry("Java World")));
        assertTrue(Predicates.ilike(null, "%world").apply(new DummyEntry("Java World")));
        assertFalse(Predicates.ilike(null, "Java_World").apply(new DummyEntry("gava World")));
    }

    void assertThis(boolean expected, String function, Comparable value, Object... args) {
        try {
            Class[] types = new Class[args.length];
            types[0] = String.class;
            for (int i = 1; i < types.length; i++) {
                types[i] = Comparable.class;
            }
            Predicate predicate = (Predicate) Predicates.class.getMethod(function, types).invoke(null, args);
            boolean result = predicate.apply(new DummyEntry(value));
            if (expected) {
                assertTrue(result);
            } else {
                assertFalse(result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    @Test
    public void testSqlPredicate() {
        assertEquals("name IN (name0,name2)", sql("name in ('name0', 'name2')"));
        assertEquals("(name LIKE 'joe' AND id=5)", sql("name like 'joe' AND id = 5"));
        assertEquals("(name REGEX '\\w*' AND id=5)", sql("name regex '\\w*' AND id = 5"));
        assertEquals("active=true", sql("active"));
        assertEquals("(active=true AND name=abc xyz 123)", sql("active AND name='abc xyz 123'"));
        assertEquals("(name LIKE 'abc-xyz+(123)' AND name=abc xyz 123)", sql("name like 'abc-xyz+(123)' AND name='abc xyz 123'"));
        assertEquals("(name REGEX '\\w{3}-\\w{3}+\\(\\d{3}\\)' AND name=abc xyz 123)", sql("name regex '\\w{3}-\\w{3}+\\(\\d{3}\\)' AND name='abc xyz 123'"));
        assertEquals("(active=true AND age>4)", sql("active and age > 4"));
        assertEquals("(active=true AND age>4)", sql("active and age>4"));
        assertEquals("(active=false AND age<=4)", sql("active=false AND age<=4"));
        assertEquals("(active=false AND age<=4)", sql("active= false and age <= 4"));
        assertEquals("(active=false AND age>=4)", sql("active=false AND (age>=4)"));
        assertEquals("(active=false OR age>=4)", sql("active =false or (age>= 4)"));
        assertEquals("name LIKE 'J%'", sql("name like 'J%'"));
        assertEquals("name REGEX 'J.*'", sql("name regex 'J.*'"));
        assertEquals("NOT(name LIKE 'J%')", sql("name not like 'J%'"));
        assertEquals("NOT(name REGEX 'J.*')", sql("name not regex 'J.*'"));
        assertEquals("(active=false OR name LIKE 'J%')", sql("active =false or name like 'J%'"));
        assertEquals("(active=false OR name LIKE 'Java World')", sql("active =false or name like 'Java World'"));
        assertEquals("(active=false OR name LIKE 'Java W% Again')", sql("active =false or name like 'Java W% Again'"));
        assertEquals("(active=false OR name REGEX 'J.*')", sql("active =false or name regex 'J.*'"));
        assertEquals("(active=false OR name REGEX 'Java World')", sql("active =false or name regex 'Java World'"));
        assertEquals("(active=false OR name REGEX 'Java W.* Again')", sql("active =false or name regex 'Java W.* Again'"));
        assertEquals("i<=-1", sql("i<= -1"));
        assertEquals("age IN (-1)", sql("age in (-1)"));
        assertEquals("age IN (10,15)", sql("age in (10, 15)"));
        assertEquals("NOT(age IN (10,15))", sql("age not in ( 10 , 15 )"));
        assertEquals("(active=true AND age BETWEEN 10 AND 15)", sql("active and age between 10 and 15"));
        assertEquals("(age IN (10,15) AND active=true)", sql("age IN (10, 15) and active"));
        assertEquals("(active=true OR age IN (10,15))", sql("active or (age in ( 10,15))"));
        assertEquals("(age>10 AND (active=true OR age IN (10,15)))", sql("age>10 AND (active or (age IN (10, 15 )))"));
        assertEquals("(age<=10 AND (active=true OR NOT(age IN (10,15))))", sql("age<=10 AND (active or (age not in (10 , 15)))"));
        assertEquals("age BETWEEN 10 AND 15", sql("age between 10 and 15"));
        assertEquals("NOT(age BETWEEN 10 AND 15)", sql("age not between 10 and 15"));
        assertEquals("(active=true AND age BETWEEN 10 AND 15)", sql("active and age between 10 and 15"));
        assertEquals("(age BETWEEN 10 AND 15 AND active=true)", sql("age between 10 and 15 and active"));
        assertEquals("(active=true OR age BETWEEN 10 AND 15)", sql("active or (age between 10 and 15)"));
        assertEquals("(age>10 AND (active=true OR age BETWEEN 10 AND 15))", sql("age>10 AND (active or (age between 10 and 15))"));
        assertEquals("(age<=10 AND (active=true OR NOT(age BETWEEN 10 AND 15)))", sql("age<=10 AND (active or (age not between 10 and 15))"));
        assertEquals("name ILIKE 'J%'", sql("name ilike 'J%'"));
        //issue #594
        assertEquals("(name IN (name0,name2) AND age IN (2,5,8))", sql("name in('name0', 'name2') and age   IN ( 2, 5  ,8)"));
    }

    @Test
    public void testSqlPredicateEscape() {
        assertEquals("(active=true AND name=abc x'yz 1'23)", sql("active AND name='abc x''yz 1''23'"));
        assertEquals("(active=true AND name=)", sql("active AND name=''"));

        assertTrue(Predicates.like(null, "J.-*.*\\%").apply(new DummyEntry("J.-*.*%")));
        assertTrue(Predicates.like(null, "J\\_").apply(new DummyEntry("J_")));

        assertTrue(Predicates.like(null, "J%").apply(new DummyEntry("Java")));
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidSqlPredicate1() {
        new SqlPredicate("invalid sql");
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidSqlPredicate2() {
        new SqlPredicate("");
    }

    private String sql(String sql) {
        return new SqlPredicate(sql).toString();
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
        public Comparable getAttribute(String attributeName) throws com.hazelcast.query.impl.QueryException {
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

    private static Map.Entry createEntry(final Object key, final Object value) {
        return new QueryEntry(null, toData(key), key, value);
    }
}
