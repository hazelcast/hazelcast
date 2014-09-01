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

import com.hazelcast.query.Predicate;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.impl.DateHelperTest;
import com.hazelcast.query.impl.QueryEntry;
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
import static com.hazelcast.query.SampleObjects.Employee;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SqlPredicateTest {
    @Test
    public void testEqualsWhenSqlMatches() throws Exception {
        Predicate sql1 = SqlPredicate.createPredicate("foo='bar'");
        Predicate sql2 = SqlPredicate.createPredicate("foo='bar'");
        assertEquals(sql1, sql2);
    }

    @Test
    public void testEqualsWhenSqlDifferent() throws Exception {
        Predicate sql1 = SqlPredicate.createPredicate("foo='bar'");
        Predicate sql2 = SqlPredicate.createPredicate("foo='baz'");
        assertNotEquals(sql1, sql2);
    }

    @Test
    public void testEqualsNull() throws Exception {
        Predicate sql = SqlPredicate.createPredicate("foo='bar'");
        assertNotEquals(sql, null);
    }

    @Test
    public void testEqualsSameObject() throws Exception {
        Predicate sql = SqlPredicate.createPredicate("foo='bar'");
        assertEquals(sql, sql);
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
        Employee value = createValue();

        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DateHelperTest.DATE_FORMAT, Locale.US);
        assertSqlTrue("createDate >= '" + simpleDateFormat.format(new Date(0)) + "'", value);
        assertSqlTrue("sqlDate >= '" + new java.sql.Date(0) + "'", value);
        assertSqlTrue("date >= '" + new Timestamp(0) + "'", value);
    }

    @Test
    public void testSql_withBigDecimal() {
        Employee value = createValue();

        assertSqlTrue("bigDecimal > '" + new BigDecimal("1.23E2") + "'", value);
        assertSqlTrue("bigDecimal >= '" + new BigDecimal("1.23E3") + "'", value);
        assertSqlFalse("bigDecimal = '" + new BigDecimal("1.23") + "'", value);
        assertSqlTrue("bigDecimal = '1.23E3'", value);
        assertSqlTrue("bigDecimal = 1.23E3", value);
        assertSqlFalse("bigDecimal = 1.23", value);
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
        Employee value = new Employee("abc-123-xvz", 34, true, 10D);
        assertSqlTrue("(age >= " + 20 + ") AND (age <= " + 40 + ")", value);
        assertSqlTrue("(age >= " + 20 + ") AND (age <= " + 34 + ")", value);
        assertSqlTrue("(age >= " + 34 + ") AND (age <= " + 35 + ")", value);
        assertSqlTrue("age IN (" + 34 + ", " + 35 + ")", value);
        assertSqlFalse("age = 33", value);
        assertSqlTrue("age = 34", value);
        assertSqlTrue("age > 5", value);
        assertSqlTrue("age = -33", createValue(-33));
    }


    @Test
    public void testSql_withDouble() {
        Employee value = new Employee("abc-123-xvz", 34, true, 10D);
        assertSqlTrue("salary > 5", value);
        assertSqlTrue("salary > 5 and salary < 11", value);
        assertSqlFalse("salary > 15 or salary < 10", value);
        assertSqlTrue("salary between 9.99 and 10.01", value);
        assertSqlTrue("salary between 5 and 15", value);

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
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidSqlPredicate1() {
        SqlPredicate.createPredicate("invalid sql");
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidSqlPredicate2() {
        SqlPredicate.createPredicate("");
    }

    private String sql(String sql) {
        return SqlPredicate.createPredicate(sql).toString();
    }

    private static Map.Entry createEntry(final Object key, final Object value) {
        return new QueryEntry(null, toData(key), key, value);
    }

    private void assertSqlTrue(String s, Object value) {
        final Predicate predicate = SqlPredicate.createPredicate(s);
        final Map.Entry entry = createEntry("1", value);
        assertTrue(predicate.apply(entry));
    }

    private void assertSqlFalse(String s, Object value) {
        final Predicate predicate = SqlPredicate.createPredicate(s);
        final Map.Entry entry = createEntry("1", value);
        assertFalse(predicate.apply(entry));
    }

    private Employee createValue() {
        return new Employee("abc-123-xvz", 34, true, 10D);
    }

    private Employee createValue(String name) {
        return new Employee(name, 34, true, 10D);
    }

    private Employee createValue(int age) {
        return new Employee("abc-123-xvz", age, true, 10D);
    }
}
