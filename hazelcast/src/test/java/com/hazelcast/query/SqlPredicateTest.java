/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.query.SampleObjects.ObjectWithBigDecimal;
import com.hazelcast.query.SampleObjects.ObjectWithBoolean;
import com.hazelcast.query.SampleObjects.ObjectWithByte;
import com.hazelcast.query.SampleObjects.ObjectWithChar;
import com.hazelcast.query.SampleObjects.ObjectWithDate;
import com.hazelcast.query.SampleObjects.ObjectWithDouble;
import com.hazelcast.query.SampleObjects.ObjectWithFloat;
import com.hazelcast.query.SampleObjects.ObjectWithInteger;
import com.hazelcast.query.SampleObjects.ObjectWithLong;
import com.hazelcast.query.SampleObjects.ObjectWithShort;
import com.hazelcast.query.SampleObjects.ObjectWithSqlDate;
import com.hazelcast.query.SampleObjects.ObjectWithSqlTimestamp;
import com.hazelcast.query.SampleObjects.ObjectWithUUID;
import com.hazelcast.query.impl.DateHelperTest;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import com.hazelcast.query.impl.predicates.RegexPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.TestUtil.toData;
import static com.hazelcast.test.HazelcastTestSupport.assertInstanceOf;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SqlPredicateTest {

    final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    static final String[] TEST_MATCHING_SQL_PREDICATES = new String[] {
            "name = 'Joe' and age = 25 and (city = 'austin' or city = 'AUSTIN')",
            "name = 'Joe' or city = 'Athens'",
            "(name = 'Jane' or name = 'Joe' or city = 'AUSTIN') and age = 25",
            "(name = 'Jane' or name = 'Joe' or city = 'AUSTIN') and age = 25 and salary = 0",
            "(name = 'Jane' or name = 'Joe') and age = 25 and salary = 0 or age = 24",
            "name = 'Jane' or age = 25 and name = 'Joe'", // correct precedence is "name = 'Jane' or (age = 25 and name = 'Joe')
            "age = 35 or age = 24 or age = 31 or (name = 'Joe' and age = 25)",
            };

    static final String[] TEST_NOT_MATCHING_SQL_PREDICATES = new String[] {
            "name = 'Joe' and age = 21 and (city = 'austin' or city = 'ATHENS')",
            "name = 'Jane' or city = 'Athens'",
            "(name = 'Jane' or name = 'Catie' or city = 'San Jose') and age = 25",
            "(name = 'Joe' or name = 'Catie' or city = 'San Jose') and age = 21",
            "(name = 'Jane' or name = 'Joe' or city = 'AUSTIN') and age = 25 and salary = 10",
            "(name = 'Jane' or name = 'Catie' or city = 'San Jose') and age = 25 and salary = 10",
            "(name = 'Jane' or name = 'Joe') and age = 25 and salary = 13 or age = 24",
            "name = 'Jane' or age = 25 and name = 'Catie'",
            "age = 35 or age = 24 or age = 31 or (name = 'Joe' and age = 27)",
            };

    @Test
    public void testSqlPredicates() {
        Employee employee = new Employee("Joe", "AUSTIN", 25, true, 0);
        for (String s : TEST_MATCHING_SQL_PREDICATES) {
            try {
                assertSqlTrue(s, employee);
            }
            catch (AssertionError assertionError) {
                fail("Failed matching SQL predicate \"" + s + '"');
            }
        }
        for (String s : TEST_NOT_MATCHING_SQL_PREDICATES) {
            try {
                assertSqlFalse(s, employee);
            }
            catch (AssertionError assertionError) {
                fail("Failed non-matching SQL predicate \"" + s + '"');
            }
        }
    }

    public static class Record {
        private String str1, str2, str3;

        public Record(String str1, String str2, String str3) {
            this.str1 = str1;
            this.str2 = str2;
            this.str3 = str3;
        }

        public String getStr1() {
            return str1;
        }

        public void setStr1(String str1) {
            this.str1 = str1;
        }

        public String getStr2() {
            return str2;
        }

        public void setStr2(String str2) {
            this.str2 = str2;
        }

        public String getStr3() {
            return str3;
        }

        public void setStr3(String str3) {
            this.str3 = str3;
        }
    }

    // ZD issue 1950
    @Test
    public void testRecordPredicate() {
        Record record = new Record("ONE", "TWO", "THREE");
        SqlPredicate predicate = new SqlPredicate("str1 = 'ONE' AND str2 = 'TWO' AND (str3 = 'THREE' OR str3 = 'three')");
        Map.Entry entry = createEntry("1", record);
        assertTrue(predicate.apply(entry));
    }

    @Test
    public void testEqualsWhenSqlMatches() {
        SqlPredicate sql1 = new SqlPredicate("foo='bar'");
        SqlPredicate sql2 = new SqlPredicate("foo='bar'");
        assertEquals(sql1, sql2);
    }

    @Test
    public void testEqualsWhenSqlDifferent() {
        SqlPredicate sql1 = new SqlPredicate("foo='bar'");
        SqlPredicate sql2 = new SqlPredicate("foo='baz'");
        assertNotEquals(sql1, sql2);
    }

    @Test
    public void testEqualsNull() {
        SqlPredicate sql = new SqlPredicate("foo='bar'");
        assertNotEquals(sql, null);
    }

    @Test
    public void testEqualsSameObject() {
        SqlPredicate sql = new SqlPredicate("foo='bar'");
        assertEquals(sql, sql);
    }

    @Test
    public void testHashCode() {
        SqlPredicate sql = new SqlPredicate("foo='bar'");
        assertEquals("foo='bar'".hashCode(), sql.hashCode());
    }

    @Test
    public void testSql_withEnum() {
        Employee value = createValue();
        value.setState(SampleObjects.State.STATE2);
        Employee nullNameValue = createValue(null);

        assertSqlTrue("state == TestUtil.State.STATE2", value);
        assertSqlTrue("state == " + SampleObjects.State.STATE2, value);
        assertSqlFalse("state == TestUtil.State.STATE1", value);
        assertSqlFalse("state == TestUtil.State.STATE1", nullNameValue);
        assertSqlTrue("state == NULL", nullNameValue);
    }

    @Test
    public void testSql_withDate() {
        Date date = new Date();
        ObjectWithDate value = new ObjectWithDate(date);
        SimpleDateFormat format = new SimpleDateFormat(DateHelperTest.DATE_FORMAT, Locale.US);
        assertSqlTrue("attribute > '" + format.format(new Date(0)) + "'", value);
        assertSqlTrue("attribute >= '" + format.format(new Date(0)) + "'", value);
        assertSqlTrue("attribute < '" + format.format(new Date(date.getTime() + 1000)) + "'", value);
        assertSqlTrue("attribute <= '" + format.format(new Date(date.getTime() + 1000)) + "'", value);
    }

    @Test
    public void testSql_withSqlDate() {
        java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
        ObjectWithSqlDate value = new ObjectWithSqlDate(date);
        SimpleDateFormat format = new SimpleDateFormat(DateHelperTest.SQL_DATE_FORMAT, Locale.US);
        assertSqlTrue("attribute > '" + format.format(new java.sql.Date(0)) + "'", value);
        assertSqlTrue("attribute >= '" + format.format(new java.sql.Date(0)) + "'", value);
        assertSqlTrue("attribute < '" + format.format(new java.sql.Date(date.getTime() + TimeUnit.DAYS.toMillis(2))) + "'", value);
        assertSqlTrue("attribute <= '" + format.format(new java.sql.Date(date.getTime() + TimeUnit.DAYS.toMillis(2))) + "'", value);
    }

    @Test
    public void testSql_withTimestamp() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        ObjectWithSqlTimestamp value = new ObjectWithSqlTimestamp(timestamp);
        SimpleDateFormat format = new SimpleDateFormat(DateHelperTest.TIMESTAMP_FORMAT, Locale.US);
        assertSqlTrue("attribute > '" + format.format(new Timestamp(0)) + "'", value);
        assertSqlTrue("attribute >= '" + format.format(new Timestamp(0)) + "'", value);
        assertSqlTrue("attribute < '" + format.format(new Timestamp(timestamp.getTime() + 1000)) + "'", value);
        assertSqlTrue("attribute <= '" + format.format(new Timestamp(timestamp.getTime() + 1000)) + "'", value);
    }

    @Test
    public void testSql_withBigDecimal() {
        ObjectWithBigDecimal value = new ObjectWithBigDecimal(new BigDecimal("1.23E3"));
        assertSqlTrue("attribute > '" + new BigDecimal("1.23E2") + "'", value);
        assertSqlTrue("attribute >= '" + new BigDecimal("1.23E3") + "'", value);
        assertSqlFalse("attribute = '" + new BigDecimal("1.23") + "'", value);
        assertSqlTrue("attribute = '1.23E3'", value);
        assertSqlTrue("attribute = 1.23E3", value);
        assertSqlFalse("attribute = 1.23", value);
    }

    @Test
    public void testSql_withBigInteger() {
        SampleObjects.ObjectWithBigInteger value = new SampleObjects.ObjectWithBigInteger(new BigInteger("123"));
        assertSqlTrue("attribute > '" + new BigInteger("122") + "'", value);
        assertSqlTrue("attribute >= '" + new BigInteger("123") + "'", value);
        assertSqlTrue("attribute = '" + new BigInteger("123") + "'", value);
        assertSqlFalse("attribute = '" + new BigInteger("122") + "'", value);
        assertSqlTrue("attribute < '" + new BigInteger("124") + "'", value);
        assertSqlTrue("attribute <= '" + new BigInteger("123") + "'", value);
        assertSqlTrue("attribute = 123", value);
        assertSqlTrue("attribute = '123'", value);
        assertSqlTrue("attribute != 124", value);
        assertSqlTrue("attribute <> 124", value);
        assertSqlFalse("attribute = 124", value);
        assertSqlTrue("attribute between 122 and 124", value);
        assertSqlTrue("attribute in (122, 123, 124)", value);
    }

    @Test
    public void testSql_withString() {
        Employee value = createValue();
        Employee nullNameValue = new Employee(null, 34, true, 10D);

        assertSqlFalse("name = 'null'", nullNameValue);
        assertSqlTrue("name = null", nullNameValue);
        assertSqlTrue("name = NULL", nullNameValue);
        assertSqlTrue("name != null", value);
        assertSqlTrue("name != NULL", value);
        assertSqlTrue("name <> null", value);
        assertSqlTrue("name <> NULL", value);
        assertSqlTrue(" (name LIKE 'abc-%') AND (age <= " + 40 + ")", value);
        assertSqlTrue(" (name REGEX 'abc-.*') AND (age <= " + 40 + ")", value);

        assertSqlTrue("name='abc-123-xvz'", value);
        assertSqlTrue("name='abc 123-xvz'", createValue("abc 123-xvz"));
        assertSqlTrue("name='abc 123-xvz+(123)'", createValue("abc 123-xvz+(123)"));
        assertSqlFalse("name='abc 123-xvz+(123)'", createValue("abc123-xvz+(123)"));
        assertSqlTrue("name LIKE 'abc-%'", createValue("abc-123"));
        assertSqlTrue("name REGEX '^\\w{3}-\\d{3}-\\w{3}$'", value);
        assertSqlFalse("name REGEX '^[^\\w]{3}-\\d{3}-\\w{3}$'", value);
        assertSqlTrue(" (name ILIKE 'ABC-%') AND (age <= " + 40 + ")", value);
    }

    @Test
    public void testSql_withInteger() {
        ObjectWithInteger value = new ObjectWithInteger(34);
        assertSqlTrue("(attribute >= 20) AND (attribute <= 40)", value);
        assertSqlTrue("(attribute >= 20 ) AND (attribute <= 34)", value);
        assertSqlTrue("(attribute >= 34) AND (attribute <= 35)", value);
        assertSqlTrue("attribute IN (34, 35)", value);
        assertSqlFalse("attribute IN (33,35)", value);
        assertSqlFalse("attribute = 33", value);
        assertSqlTrue("attribute = 34", value);
        assertSqlTrue("attribute > 5", value);
        assertSqlTrue("attribute = -33", new ObjectWithInteger(-33));
    }

    @Test
    public void testSql_withLong() {
        ObjectWithLong value = new ObjectWithLong(34L);
        assertSqlTrue("(attribute >= 20) AND (attribute <= 40)", value);
        assertSqlTrue("(attribute >= 20 ) AND (attribute <= 34)", value);
        assertSqlTrue("(attribute >= 34) AND (attribute <= 35)", value);
        assertSqlTrue("attribute IN (34, 35)", value);
        assertSqlFalse("attribute IN (33,35)", value);
        assertSqlFalse("attribute = 33", value);
        assertSqlTrue("attribute = 34", value);
        assertSqlTrue("attribute > 5", value);
        assertSqlTrue("attribute = -33", new ObjectWithLong(-33));
    }

    @Test
    public void testSql_withDouble() {
        ObjectWithDouble value = new ObjectWithDouble(10.001D);
        assertSqlTrue("attribute > 5", value);
        assertSqlTrue("attribute > 5 and attribute < 11", value);
        assertSqlFalse("attribute > 15 or attribute < 10", value);
        assertSqlTrue("attribute between 9.99 and 10.01", value);
        assertSqlTrue("attribute between 5 and 15", value);
    }

    @Test
    public void testSql_withFloat() {
        ObjectWithFloat value = new ObjectWithFloat(10.001f);
        assertSqlTrue("attribute > 5", value);
        assertSqlTrue("attribute > 5 and attribute < 11", value);
        assertSqlFalse("attribute > 15 or attribute < 10", value);
        assertSqlTrue("attribute between 9.99 and 10.01", value);
        assertSqlTrue("attribute between 5 and 15", value);
    }

    @Test
    public void testSql_withShort() {
        ObjectWithShort value = new ObjectWithShort((short) 10);
        assertSqlTrue("attribute = 10", value);
        assertSqlFalse("attribute = 11", value);
        assertSqlTrue("attribute >= 10", value);
        assertSqlTrue("attribute <= 10", value);
        assertSqlTrue("attribute > 5", value);
        assertSqlTrue("attribute > 5 and attribute < 11", value);
        assertSqlFalse("attribute > 15 or attribute < 10", value);
        assertSqlTrue("attribute between 5 and 15", value);
        assertSqlTrue("attribute in (5, 10, 15)", value);
        assertSqlFalse("attribute in (5, 11, 15)", value);
    }

    @Test
    public void testSql_withByte() {
        ObjectWithByte value = new ObjectWithByte((byte) 10);
        assertSqlTrue("attribute = 10", value);
        assertSqlFalse("attribute = 11", value);
        assertSqlTrue("attribute >= 10", value);
        assertSqlTrue("attribute <= 10", value);
        assertSqlTrue("attribute > 5", value);
        assertSqlTrue("attribute > 5 and attribute < 11", value);
        assertSqlFalse("attribute > 15 or attribute < 10", value);
        assertSqlTrue("attribute between 5 and 15", value);
        assertSqlTrue("attribute in (5, 10, 15)", value);
        assertSqlFalse("attribute in (5, 11, 15)", value);
    }

    @Test
    public void testSql_withChar() {
        ObjectWithChar value = new ObjectWithChar('%');
        assertSqlTrue("attribute = '%'", value);
        assertSqlFalse("attribute = '$'", value);
        assertSqlTrue("attribute in ('A', '#', '%')", value);
        assertSqlFalse("attribute in ('A', '#', '&')", value);
    }

    @Test
    public void testSql_withBoolean() {
        ObjectWithBoolean value = new ObjectWithBoolean(true);
        assertSqlTrue("attribute", value);
        assertSqlTrue("attribute = true", value);
        assertSqlFalse("attribute = false", value);
        assertSqlFalse("not attribute", value);
    }

    @Test
    public void testSql_withUUID() {
        UUID uuid = UUID.randomUUID();
        ObjectWithUUID value = new ObjectWithUUID(uuid);
        assertSqlTrue("attribute = '" + uuid.toString() + "'", value);
        assertSqlFalse("attribute = '" + UUID.randomUUID().toString() + "'", value);
    }

    @Test
    public void testSqlPredicate() {
        assertEquals("name IN (name0,name2)", sql("name in ('name0', 'name2')"));
        assertEquals("(name LIKE 'joe' AND id=5)", sql("name like 'joe' AND id = 5"));
        assertEquals("(name REGEX '\\w*' AND id=5)", sql("name regex '\\w*' AND id = 5"));
        assertEquals("active=true", sql("active"));
        assertEquals("(active=true AND name=abc xyz 123)", sql("active AND name='abc xyz 123'"));
        assertEquals("(name LIKE 'abc-xyz+(123)' AND name=abc xyz 123)", sql("name like 'abc-xyz+(123)' AND name='abc xyz 123'"));
        assertEquals("(name REGEX '\\w{3}-\\w{3}+\\(\\d{3}\\)' AND name=abc xyz 123)",
                sql("name regex '\\w{3}-\\w{3}+\\(\\d{3}\\)' AND name='abc xyz 123'"));
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
        assertEquals("(age>10 AND (active=true OR age BETWEEN 10 AND 15))",
                sql("age>10 AND (active or (age between 10 and 15))"));
        assertEquals("(age<=10 AND (active=true OR NOT(age BETWEEN 10 AND 15)))",
                sql("age<=10 AND (active or (age not between 10 and 15))"));
        assertEquals("name ILIKE 'J%'", sql("name ilike 'J%'"));
        // issue #594
        assertEquals("(name IN (name0,name2) AND age IN (2,5,8))", sql("name in('name0', 'name2') and age   IN ( 2, 5  ,8)"));
    }

    @Test
    public void testSqlPredicateEscape() {
        assertEquals("(active=true AND name=abc x'yz 1'23)", sql("active AND name='abc x''yz 1''23'"));
        assertEquals("(active=true AND name=)", sql("active AND name=''"));
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidSqlPredicate1() {
        new SqlPredicate("invalid sql");
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidSqlPredicate2() {
        new SqlPredicate("");
    }

    // This test used to fail with a stack overflow exception, due to the way SQL predicates were
    // nested, missing an opportunity to flatten.
    // https://github.com/hazelcast/hazelcast/issues/7583
    @Test
    public void testLongPredicate() {
        TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
        HazelcastInstance hazelcastInstance = factory.newHazelcastInstance();
        IMap<Integer, Integer> map = hazelcastInstance.getMap(randomString());

        for (int i = 0; i < 8000; i++) {
            map.put(i, i);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 8000; i++) {
            sb.append("intValue() == " + i);
            sb.append(" or ");
        }
        sb.append(" intValue() == -1");

        SqlPredicate predicate = new SqlPredicate(sb.toString());

        // all entries must match
        Set<Map.Entry<Integer, Integer>> entries = map.entrySet(predicate);
        assertEquals(map.size(), entries.size());

        factory.terminateAll();
    }

    @Test
    public void testOr_whenBothPredicatesOr() {
        OrPredicate predicate1 = new OrPredicate(new SqlPredicate("a == 1"), new SqlPredicate("a == 2"));
        OrPredicate predicate2 = new OrPredicate(new SqlPredicate("a == 3"));
        OrPredicate concatenatedOr = SqlPredicate.flattenCompound(predicate1, predicate2, OrPredicate.class);
        assertEquals(3, concatenatedOr.getPredicates().length);
    }

    @Test
    public void testOr_whenLeftPredicateOr() {
        OrPredicate predicate1 = new OrPredicate(new SqlPredicate("a == 1"), new SqlPredicate("a == 2"));
        TruePredicate predicate2 = new TruePredicate();
        OrPredicate concatenatedOr = SqlPredicate.flattenCompound(predicate1, predicate2, OrPredicate.class);
        assertEquals(3, concatenatedOr.getPredicates().length);
        assertInstanceOf(SqlPredicate.class, concatenatedOr.getPredicates()[0]);
        assertInstanceOf(SqlPredicate.class, concatenatedOr.getPredicates()[1]);
        assertSame(predicate2, concatenatedOr.getPredicates()[2]);
    }

    @Test
    public void testOr_whenRightPredicateOr() {
        TruePredicate predicate1 = new TruePredicate();
        OrPredicate predicate2 = new OrPredicate(new SqlPredicate("a == 1"), new SqlPredicate("a == 2"));
        OrPredicate concatenatedOr = SqlPredicate.flattenCompound(predicate1, predicate2, OrPredicate.class);
        assertEquals(3, concatenatedOr.getPredicates().length);
        assertSame(predicate1, concatenatedOr.getPredicates()[0]);
        assertInstanceOf(SqlPredicate.class, concatenatedOr.getPredicates()[1]);
        assertInstanceOf(SqlPredicate.class, concatenatedOr.getPredicates()[2]);
    }

    @Test
    public void testOr_whenNoPredicateOr() {
        TruePredicate predicate1 = new TruePredicate();
        TruePredicate predicate2 = new TruePredicate();
        OrPredicate concatenatedOr = SqlPredicate.flattenCompound(predicate1, predicate2, OrPredicate.class);
        assertEquals(2, concatenatedOr.getPredicates().length);
        assertSame(predicate1, concatenatedOr.getPredicates()[0]);
        assertSame(predicate2, concatenatedOr.getPredicates()[1]);
    }

    @Test
    public void testAnd_whenBothPredicatesAnd() {
        AndPredicate predicate1 = new AndPredicate(new SqlPredicate("a == 1"), new SqlPredicate("a == 2"));
        AndPredicate predicate2 = new AndPredicate(new SqlPredicate("a == 3"));
        AndPredicate concatenatedOr = SqlPredicate.flattenCompound(predicate1, predicate2, AndPredicate.class);
        assertEquals(3, concatenatedOr.getPredicates().length);
    }

    @Test
    public void testAnd_whenLeftPredicateAnd() {
        AndPredicate predicate1 = new AndPredicate(new SqlPredicate("a == 1"), new SqlPredicate("a == 2"));
        TruePredicate predicate2 = new TruePredicate();
        AndPredicate concatenatedOr = SqlPredicate.flattenCompound(predicate1, predicate2, AndPredicate.class);
        assertEquals(3, concatenatedOr.getPredicates().length);
        assertInstanceOf(SqlPredicate.class, concatenatedOr.getPredicates()[0]);
        assertInstanceOf(SqlPredicate.class, concatenatedOr.getPredicates()[1]);
        assertSame(predicate2, concatenatedOr.getPredicates()[2]);
    }

    @Test
    public void testAnd_whenRightPredicateAnd() {
        TruePredicate predicate1 = new TruePredicate();
        AndPredicate predicate2 = new AndPredicate(new SqlPredicate("a == 1"), new SqlPredicate("a == 2"));
        AndPredicate concatenatedOr = SqlPredicate.flattenCompound(predicate1, predicate2, AndPredicate.class);
        assertEquals(3, concatenatedOr.getPredicates().length);
        assertSame(predicate1, concatenatedOr.getPredicates()[0]);
        assertInstanceOf(SqlPredicate.class, concatenatedOr.getPredicates()[1]);
        assertInstanceOf(SqlPredicate.class, concatenatedOr.getPredicates()[2]);
    }

    @Test
    public void testAnd_whenNoPredicateAnd() {
        TruePredicate predicate1 = new TruePredicate();
        TruePredicate predicate2 = new TruePredicate();
        AndPredicate concatenatedOr = SqlPredicate.flattenCompound(predicate1, predicate2, AndPredicate.class);
        assertEquals(2, concatenatedOr.getPredicates().length);
        assertSame(predicate1, concatenatedOr.getPredicates()[0]);
        assertSame(predicate2, concatenatedOr.getPredicates()[1]);
    }

    @Test
    // http://stackoverflow.com/questions/37382505/hazelcast-imap-valuespredicate-miss-data
    public void testAndWithRegex_stackOverflowIssue() {
        SqlPredicate sqlPredicate = new SqlPredicate("nextExecuteTime < 1463975296703 AND autoIncrementId REGEX '.*[5,6,7,8,9]$'");
        Predicate predicate = sqlPredicate.predicate;

        AndPredicate andPredicate = (AndPredicate) predicate;
        assertEquals(GreaterLessPredicate.class, andPredicate.getPredicates()[0].getClass());
        assertEquals(RegexPredicate.class, andPredicate.getPredicates()[1].getClass());
    }

    private String sql(String sql) {
        return new SqlPredicate(sql).toString();
    }

    private Map.Entry createEntry(final Object key, final Object value) {
        return new QueryEntry(serializationService, toData(key), value, Extractors.empty());
    }

    private void assertSqlTrue(String s, Object value) {
        final SqlPredicate predicate = new SqlPredicate(s);
        final Map.Entry entry = createEntry("1", value);
        assertTrue(predicate.apply(entry));
    }

    private void assertSqlFalse(String s, Object value) {
        final SqlPredicate predicate = new SqlPredicate(s);
        final Map.Entry entry = createEntry("1", value);
        assertFalse(predicate.apply(entry));
    }

    private Employee createValue() {
        return new Employee("abc-123-xvz", 34, true, 10D);
    }

    private Employee createValue(String name) {
        return new Employee(name, 34, true, 10D);
    }
}
