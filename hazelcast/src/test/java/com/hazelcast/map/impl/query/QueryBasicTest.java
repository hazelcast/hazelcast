/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableTest.ChildPortableObject;
import com.hazelcast.nio.serialization.PortableTest.GrandParentPortableObject;
import com.hazelcast.nio.serialization.PortableTest.ParentPortableObject;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.SampleTestObjects.State;
import com.hazelcast.query.SampleTestObjects.Value;
import com.hazelcast.query.SampleTestObjects.ValueType;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryBasicTest extends HazelcastTestSupport {

    @Test
    public void testPredicatedEvaluatedSingleThreadedByDefault() {
        Config config = getConfig();
        HazelcastProperties properties = new HazelcastProperties(config);
        boolean parallelEvaluation = properties.getBoolean(GroupProperty.QUERY_PREDICATE_PARALLEL_EVALUATION);
        assertFalse(parallelEvaluation);
    }

    @Test(timeout = 1000 * 90)
    public void testInPredicateWithEmptyArray() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);
        final IMap<String, Value> map = instance.getMap("default");
        for (int i = 0; i < 10; i++) {
            final Value v = new Value("name" + i, new ValueType("type" + i), i);
            map.put("" + i, v);
        }
        String[] emptyArray = new String[2];
        final Predicate predicate = new PredicateBuilder().getEntryObject().get("name").in(emptyArray);
        final Collection<Value> values = map.values(predicate);
        assertEquals(values.size(), 0);
    }

    @Test
    public void testQueryIndexNullValues() {
        final HazelcastInstance instance = createHazelcastInstance(getConfig());
        final IMap<String, Value> map = instance.getMap("default");
        map.addIndex("name", true);
        map.put("first", new Value("first", 1));
        map.put("second", new Value(null, 2));
        map.put("third", new Value(null, 3));
        final Predicate predicate = new SqlPredicate("name=null");
        final Collection<Value> values = map.values(predicate);

        final int[] expectedIndexValues = {2, 3};
        assertEquals(expectedIndexValues.length, values.size());

        final int[] actualIndexValues = new int[values.size()];
        int i = 0;
        for (Value value : values) {
            actualIndexValues[i++] = value.getIndex();
        }
        Arrays.sort(actualIndexValues);
        assertArrayEquals(expectedIndexValues, actualIndexValues);
    }

    @Test
    public void testLesserEqual() {
        final HazelcastInstance instance = createHazelcastInstance(getConfig());
        final IMap<String, Value> map = instance.getMap("default");
        map.addIndex("index", true);
        for (int i = 0; i < 10; i++) {
            map.put("" + i, new Value("" + i, i));
        }
        final Predicate predicate = new SqlPredicate("index<=5");
        final Collection<Value> values = map.values(predicate);


        final int[] expectedIndexValues = new int[6];
        for (int i = 0; i < expectedIndexValues.length; i++) {
            expectedIndexValues[i] = i;
        }
        assertEquals(expectedIndexValues.length, values.size());

        final int[] actualIndexValues = new int[values.size()];
        int i = 0;
        for (Value value : values) {
            actualIndexValues[i++] = value.getIndex();
        }
        Arrays.sort(actualIndexValues);
        assertArrayEquals(expectedIndexValues, actualIndexValues);
    }

    @Test
    public void testNotEqual() {
        final HazelcastInstance instance = createHazelcastInstance(getConfig());
        final IMap<String, Value> map = instance.getMap("default");
        map.addIndex("name", true);
        map.put("first", new Value("first", 1));
        map.put("second", new Value(null, 2));
        map.put("third", new Value(null, 3));
        final Predicate predicate = new SqlPredicate("name != null");


        final Collection<Value> values = map.values(predicate);
        final int[] expectedIndexValues = {1};
        assertEquals(expectedIndexValues.length, values.size());

        final int[] actualIndexValues = new int[values.size()];
        int i = 0;
        for (Value value : values) {
            actualIndexValues[i++] = value.getIndex();
        }
        Arrays.sort(actualIndexValues);
        assertArrayEquals(expectedIndexValues, actualIndexValues);
    }

    @Test(timeout = 1000 * 90)
    public void issue393SqlIn() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Value> map = instance.getMap("default");
        map.addIndex("name", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i);
            map.put("" + i, v);
        }
        Predicate predicate = new SqlPredicate("name IN ('name0', 'name2')");
        Collection<Value> values = map.values(predicate);
        String[] expectedValues = new String[]{"name0", "name2"};
        assertEquals(expectedValues.length, values.size());
        List<String> names = new ArrayList<String>();
        for (Value configObject : values) {
            names.add(configObject.getName());
        }
        String[] array = names.toArray(new String[0]);
        Arrays.sort(array);
        assertArrayEquals(names.toString(), expectedValues, array);
    }

    @Test(timeout = 1000 * 90)
    public void issue393SqlInInteger() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Value> map = instance.getMap("default");
        map.addIndex("index", false);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i, new ValueType("type" + i), i);
            map.put("" + i, v);
        }
        Predicate predicate = new SqlPredicate("index IN (0, 2)");
        Collection<Value> values = map.values(predicate);
        String[] expectedValues = new String[]{"name0", "name2"};
        assertEquals(expectedValues.length, values.size());
        List<String> names = new ArrayList<String>();
        for (Value configObject : values) {
            names.add(configObject.getName());
        }
        String[] array = names.toArray(new String[0]);
        Arrays.sort(array);
        assertArrayEquals(names.toString(), expectedValues, array);
    }

    @Test(timeout = 1000 * 90)
    public void testInPredicate() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, SampleTestObjects.ValueType> map = instance.getMap("testIteratorContract");
        map.put("1", new ValueType("one"));
        map.put("2", new ValueType("two"));
        map.put("3", new ValueType("three"));
        map.put("4", new ValueType("four"));
        map.put("5", new ValueType("five"));
        map.put("6", new ValueType("six"));
        map.put("7", new ValueType("seven"));
        Predicate predicate = new SqlPredicate("typeName in ('one','two')");
        for (int i = 0; i < 10; i++) {
            Collection<SampleTestObjects.ValueType> values = map.values(predicate);
            assertEquals(2, values.size());
        }
    }

    @Test(timeout = 1000 * 90)
    public void testInstanceOfPredicate() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Object> map = instance.getMap("testInstanceOfPredicate");

        LinkedList linkedList = new LinkedList();

        Predicate linkedListPredicate = Predicates.instanceOf(LinkedList.class);
        map.put("1", "someString");
        map.put("2", new ArrayList());
        map.put("3", linkedList);

        Collection<Object> values = map.values(linkedListPredicate);
        assertEquals(1, values.size());
        assertContains(values, linkedList);
    }

    @Test(timeout = 1000 * 90)
    public void testIteratorContract() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, SampleTestObjects.ValueType> map = instance.getMap("testIteratorContract");
        map.put("1", new ValueType("one"));
        map.put("2", new ValueType("two"));
        map.put("3", new ValueType("three"));
        Predicate predicate = new SqlPredicate("typeName in ('one','two')");
        assertEquals(2, map.values(predicate).size());
        assertEquals(2, map.keySet(predicate).size());
        testIterator(map.keySet().iterator(), 3);
        testIterator(map.keySet(predicate).iterator(), 2);
        testIterator(map.entrySet().iterator(), 3);
        testIterator(map.entrySet(predicate).iterator(), 2);
        testIterator(map.values().iterator(), 3);
        testIterator(map.values(predicate).iterator(), 2);
    }

    private void testIterator(Iterator it, int size) {
        for (int i = 0; i < size * 2; i++) {
            assertTrue("i is " + i, it.hasNext());
        }
        for (int i = 0; i < size; i++) {
            assertTrue(it.hasNext());
            assertNotNull(it.next());
        }
        assertFalse(it.hasNext());
        assertFalse(it.hasNext());
    }


    @Test(timeout = 1000 * 90)
    public void issue393Fail() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Value> map = instance.getMap("default");
        map.addIndex("qwe", true);
        Value v = new Value("name");
        try {
            map.put("0", v);
            fail();
        } catch (Throwable e) {
            assertContains(e.getMessage(), "There is no suitable accessor for 'qwe'");
        }
    }

    @Test(timeout = 1000 * 90)
    public void negativeDouble() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Employee> map = instance.getMap("default");
        map.addIndex("salary", false);
        map.put("" + 4, new Employee(1, "default", 1, true, -70D));
        map.put("" + 3, new Employee(1, "default", 1, true, -60D));
        map.put("" + 1, new Employee(1, "default", 1, true, -10D));
        map.put("" + 2, new Employee(2, "default", 2, true, 10D));
        Predicate predicate = new SqlPredicate("salary >= -60");
        Collection<Employee> values = map.values(predicate);
        assertEquals(3, values.size());
        predicate = new SqlPredicate("salary between -20 and 20");
        values = map.values(predicate);
        assertEquals(2, values.size());
    }

    @Test(timeout = 1000 * 90)
    public void issue393SqlEq() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Value> map = instance.getMap("default");
        map.addIndex("name", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i);
            map.put("" + i, v);
        }
        Predicate predicate = new SqlPredicate("name='name0'");
        Collection<Value> values = map.values(predicate);
        String[] expectedValues = new String[]{"name0"};
        assertEquals(expectedValues.length, values.size());
        List<String> names = new ArrayList<String>();
        for (Value configObject : values) {
            names.add(configObject.getName());
        }
        String[] array = names.toArray(new String[0]);
        Arrays.sort(array);
        assertArrayEquals(names.toString(), expectedValues, array);
    }

    @Test(timeout = 1000 * 90)
    public void issue393() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Value> map = instance.getMap("default");
        map.addIndex("name", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i);
            map.put("" + i, v);
        }
        Predicate predicate = new PredicateBuilder().getEntryObject().get("name").in("name0", "name2");
        Collection<Value> values = map.values(predicate);
        String[] expectedValues = new String[]{"name0", "name2"};
        assertEquals(expectedValues.length, values.size());
        List<String> names = new ArrayList<String>();
        for (Value configObject : values) {
            names.add(configObject.getName());
        }
        String[] array = names.toArray(new String[0]);
        Arrays.sort(array);
        assertArrayEquals(names.toString(), expectedValues, array);
    }

    @Test(timeout = 1000 * 90)
    public void testWithDashInTheNameAndSqlPredicate() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Employee> map = instance.getMap("employee");
        Employee toto = new Employee("toto", 23, true, 165765.0);
        map.put("1", toto);
        Employee toto2 = new Employee("toto-super+hero", 23, true, 165765.0);
        map.put("2", toto2);
        // works well
        Set<Map.Entry<String, Employee>> entries = map.entrySet(new SqlPredicate("name='toto-super+hero'"));
        assertTrue(entries.size() > 0);
        for (Map.Entry<String, Employee> entry : entries) {
            Employee e = entry.getValue();
            assertEquals(e, toto2);
        }
    }

    @Test(timeout = 1000 * 90)
    public void queryWithThis() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap("queryWithThis");
        map.addIndex("this", false);
        for (int i = 0; i < 1000; i++) {
            map.put("" + i, "" + i);
        }
        Predicate predicate = new PredicateBuilder().getEntryObject().get("this").equal("10");
        Collection<String> set = map.values(predicate);
        assertEquals(1, set.size());
        assertEquals(1, map.values(new SqlPredicate("this=15")).size());
    }

    /**
     * Test for issue 711
     */
    @Test(timeout = 1000 * 90)
    public void testPredicateWithEntryKeyObject() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Integer> map = instance.getMap("testPredicateWithEntryKeyObject");
        map.put("1", 11);
        map.put("2", 22);
        map.put("3", 33);
        map.put("4", 44);
        map.put("5", 55);
        map.put("6", 66);
        Predicate predicate = new PredicateBuilder().getEntryObject().key().equal("1");
        assertEquals(1, map.values(predicate).size());
        predicate = new PredicateBuilder().getEntryObject().key().in("2", "3");
        assertEquals(2, map.keySet(predicate).size());
        predicate = new PredicateBuilder().getEntryObject().key().in("2", "3", "5", "6", "7");
        assertEquals(4, map.keySet(predicate).size());
    }

    /**
     * Github issues 98 and 131
     */
    @Test(timeout = 1000 * 90)
    public void testPredicateStringAttribute() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Value> map = instance.getMap("testPredicateStringWithString");
        testPredicateStringAttribute(map);
    }

    /**
     * Github issues 98 and 131
     */
    @Test(timeout = 1000 * 90)
    public void testPredicateStringAttributesWithIndex() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Value> map = instance.getMap("testPredicateStringWithStringIndex");
        map.addIndex("name", false);
        testPredicateStringAttribute(map);
    }

    private void testPredicateStringAttribute(IMap<Integer, Value> map) {
        map.put(1, new Value("abc"));
        map.put(2, new Value("xyz"));
        map.put(3, new Value("aaa"));
        map.put(4, new Value("zzz"));
        map.put(5, new Value("klm"));
        map.put(6, new Value("prs"));
        map.put(7, new Value("prs"));
        map.put(8, new Value("def"));
        map.put(9, new Value("qwx"));
        assertEquals(8, map.values(new SqlPredicate("name > 'aac'")).size());
        assertEquals(9, map.values(new SqlPredicate("name between 'aaa' and 'zzz'")).size());
        assertEquals(7, map.values(new SqlPredicate("name < 't'")).size());
        assertEquals(6, map.values(new SqlPredicate("name >= 'gh'")).size());
        assertEquals(8, map.values(new PredicateBuilder().getEntryObject().get("name").greaterThan("aac")).size());
        assertEquals(9, map.values(new PredicateBuilder().getEntryObject().get("name").between("aaa", "zzz")).size());
        assertEquals(7, map.values(new PredicateBuilder().getEntryObject().get("name").lessThan("t")).size());
        assertEquals(6, map.values(new PredicateBuilder().getEntryObject().get("name").greaterEqual("gh")).size());
    }

    @Test(timeout = 1000 * 90)
    public void testPredicateDateAttribute() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Date> map = instance.getMap("testPredicateDateAttribute");
        testPredicateDateAttribute(map);
    }

    @Test(timeout = 1000 * 90)
    public void testPredicateDateAttributeWithIndex() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Date> map = instance.getMap("testPredicateDateAttribute");
        map.addIndex("this", true);
        testPredicateDateAttribute(map);
    }

    private void testPredicateDateAttribute(IMap<Integer, Date> map) {
        Calendar cal = Calendar.getInstance();
        cal.set(2012, Calendar.JUNE, 5);
        map.put(1, cal.getTime());
        cal.set(2011, Calendar.NOVEMBER, 10);
        map.put(2, cal.getTime());
        cal.set(2011, Calendar.FEBRUARY, 1);
        map.put(3, cal.getTime());
        cal.set(2010, Calendar.SEPTEMBER, 5);
        map.put(4, cal.getTime());
        cal.set(2000, Calendar.JUNE, 5);
        map.put(5, cal.getTime());
        cal.set(2011, Calendar.JANUARY, 1);
        assertEquals(3, map.values(new PredicateBuilder().getEntryObject().get("this").greaterThan(cal.getTime())).size());
        assertEquals(3, map.values(new SqlPredicate("this > 'Sat Jan 01 11:43:05 EET 2011'")).size());
        assertEquals(2, map.values(new PredicateBuilder().getEntryObject().get("this").lessThan(cal.getTime())).size());
        assertEquals(2, map.values(new SqlPredicate("this < 'Sat Jan 01 11:43:05 EET 2011'")).size());
        cal.set(2003, Calendar.NOVEMBER, 10);
        Date d1 = cal.getTime();
        cal.set(2012, Calendar.FEBRUARY, 10);
        Date d2 = cal.getTime();
        assertEquals(3, map.values(new PredicateBuilder().getEntryObject().get("this").between(d1, d2)).size());
        assertEquals(3, map.values(new SqlPredicate("this between 'Mon Nov 10 11:43:05 EET 2003'"
                + " and 'Fri Feb 10 11:43:05 EET 2012'")).size());
    }

    @Test(timeout = 1000 * 90)
    public void testPredicateEnumAttribute() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, NodeType> map = instance.getMap("testPredicateEnumAttribute");
        testPredicateEnumAttribute(map);
    }

    @Test(timeout = 1000 * 90)
    public void testPredicateEnumAttributeWithIndex() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, NodeType> map = instance.getMap("testPredicateEnumAttribute");
        map.addIndex("this", true);
        testPredicateEnumAttribute(map);
    }

    private void testPredicateEnumAttribute(IMap<Integer, NodeType> map) {
        map.put(1, NodeType.MEMBER);
        map.put(2, NodeType.LITE_MEMBER);
        map.put(3, NodeType.JAVA_CLIENT);
        assertEquals(NodeType.MEMBER, map.values(new SqlPredicate("this=MEMBER")).iterator().next());
        assertEquals(2, map.values(new SqlPredicate("this in (MEMBER, LITE_MEMBER)")).size());
        assertEquals(NodeType.JAVA_CLIENT,
                map.values(new PredicateBuilder().getEntryObject()
                        .get("this").equal(NodeType.JAVA_CLIENT)).iterator().next());
        assertEquals(0, map.values(new PredicateBuilder().getEntryObject()
                .get("this").equal(NodeType.CSHARP_CLIENT)).size());
        assertEquals(2, map.values(new PredicateBuilder().getEntryObject()
                .get("this").in(NodeType.LITE_MEMBER, NodeType.MEMBER)).size());
    }

    private enum NodeType {
        MEMBER,
        LITE_MEMBER,
        JAVA_CLIENT,
        CSHARP_CLIENT
    }

    @Test(timeout = 1000 * 90)
    public void testPredicateCustomAttribute() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, CustomObject> map = instance.getMap("testPredicateCustomAttribute");

        CustomAttribute attribute = new CustomAttribute(78, 145);
        CustomObject customObject = new CustomObject("name1", UuidUtil.newUnsecureUUID(), attribute);
        map.put(1, customObject);

        CustomObject object2 = new CustomObject("name2", UuidUtil.newUnsecureUUID(), attribute);
        map.put(2, object2);

        assertEquals(customObject, map.values(new PredicateBuilder().getEntryObject().get("uuid").equal(customObject.getUuid()))
                .iterator().next());
        assertEquals(2, map.values(new PredicateBuilder().getEntryObject().get("attribute").equal(attribute)).size());

        assertEquals(object2, map.values(new PredicateBuilder().getEntryObject().get("uuid").in(object2.getUuid()))
                .iterator().next());
        assertEquals(2, map.values(new PredicateBuilder().getEntryObject().get("attribute").in(attribute)).size());
    }

    public static void doFunctionalSQLQueryTest(IMap<String, Employee> map) {
        map.put("1", new Employee("joe", 33, false, 14.56));
        map.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            map.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), i));
        }
        Set<Map.Entry<String, Employee>> entries = map.entrySet();
        assertEquals(102, entries.size());

        int entryCount = 0;
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertNotNull(employee);
            entryCount++;
        }
        assertEquals(102, entryCount);

        entries = map.entrySet(new SqlPredicate("active=true and age=23"));
        assertEquals(3, entries.size());
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertEquals(employee.getAge(), 23);
            assertTrue(employee.isActive());
        }
        map.remove("2");
        entries = map.entrySet(new SqlPredicate("active=true and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertEquals(employee.getAge(), 23);
            assertTrue(employee.isActive());
        }
        entries = map.entrySet(new SqlPredicate("age!=33"));
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertTrue(employee.getAge() != 33);
        }
        entries = map.entrySet(new SqlPredicate("active!=false"));
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertTrue(employee.isActive());
        }
    }

    public static void doFunctionalQueryTest(IMap<String, Employee> map) {
        map.put("1", new Employee("joe", 33, false, 14.56));
        map.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            map.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), i));
        }
        Set<Map.Entry<String, Employee>> entries = map.entrySet();
        assertEquals(102, entries.size());

        int entryCount = 0;
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertNotNull(employee);
            entryCount++;
        }
        assertEquals(102, entryCount);

        EntryObject entryObject = new PredicateBuilder().getEntryObject();
        Predicate predicate = entryObject.is("active").and(entryObject.get("age").equal(23));
        entries = map.entrySet(predicate);
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertEquals(employee.getAge(), 23);
            assertTrue(employee.isActive());
        }
        map.remove("2");
        entries = map.entrySet(predicate);
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertEquals(employee.getAge(), 23);
            assertTrue(employee.isActive());
        }
        entries = map.entrySet(new SqlPredicate(" (age >= " + 30 + ") AND (age <= " + 40 + ")"));
        assertEquals(23, entries.size());
        for (Map.Entry entry : entries) {
            Employee employee = (Employee) entry.getValue();
            assertTrue(employee.getAge() >= 30);
            assertTrue(employee.getAge() <= 40);
        }
    }

    @Test(timeout = 1000 * 90)
    public void testInvalidSqlPredicate() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Employee> map = instance.getMap("employee");
        map.put(1, new Employee("e", 1, false, 0));
        map.put(2, new Employee("e2", 1, false, 0));
        try {
            map.values(new SqlPredicate("invalid_sql"));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertContains(e.getMessage(), "There is no suitable accessor for 'invalid_sql'");
        }
        try {
            map.values(new SqlPredicate("invalid sql"));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertContains(e.getMessage(), "Invalid SQL: [invalid sql]");
        }
        try {
            map.values(new SqlPredicate("invalid and sql"));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertContains(e.getMessage(), "There is no suitable accessor for 'invalid'");
        }
        try {
            map.values(new SqlPredicate("invalid sql and"));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertContains(e.getMessage(), "There is no suitable accessor for 'invalid'");
        }
        try {
            map.values(new SqlPredicate(""));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertContains(e.getMessage(), "Invalid SQL: []");
        }
        assertEquals(2, map.values(new SqlPredicate("age=1 and name like 'e%'")).size());
    }

    @Test(timeout = 1000 * 90)
    public void testIndexingEnumAttributeIssue597() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Value> map = instance.getMap("default");
        map.addIndex("state", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value(i % 2 == 0 ? State.STATE1 : State.STATE2, new ValueType(), i);
            map.put(i, v);
        }
        Predicate predicate = new PredicateBuilder().getEntryObject().get("state").equal(State.STATE1);
        Collection<Value> values = map.values(predicate);
        int[] expectedValues = new int[]{0, 2};
        assertEquals(expectedValues.length, values.size());
        int[] indexes = new int[2];
        int index = 0;
        for (Value configObject : values) {
            indexes[index++] = configObject.getIndex();
        }
        Arrays.sort(indexes);
        assertArrayEquals(indexes, expectedValues);
    }

    /**
     * see pull request 616
     */
    @Test(timeout = 1000 * 90)
    public void testIndexingEnumAttributeWithSqlIssue597() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Value> map = instance.getMap("default");
        map.addIndex("state", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value(i % 2 == 0 ? State.STATE1 : State.STATE2, new ValueType(), i);
            map.put(i, v);
        }

        Collection<Value> values = map.values(new SqlPredicate("state = 'STATE1'"));
        int[] expectedValues = new int[]{0, 2};
        assertEquals(expectedValues.length, values.size());
        int[] indexes = new int[2];
        int index = 0;
        for (Value configObject : values) {
            indexes[index++] = configObject.getIndex();
        }
        Arrays.sort(indexes);
        assertArrayEquals(indexes, expectedValues);
    }

    @Test(timeout = 1000 * 90)
    public void testMultipleOrPredicatesIssue885WithoutIndex() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());
        factory.newHazelcastInstance(new Config());
        IMap<Integer, Employee> map = instance.getMap("default");
        testMultipleOrPredicates(map);
    }

    @Test(timeout = 1000 * 90)
    public void testMultipleOrPredicatesIssue885WithIndex() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());
        factory.newHazelcastInstance(new Config());
        IMap<Integer, Employee> map = instance.getMap("default");
        map.addIndex("name", true);
        testMultipleOrPredicates(map);
    }

    @Test(timeout = 1000 * 90)
    public void testMultipleOrPredicatesIssue885WithDoubleIndex() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());
        factory.newHazelcastInstance(new Config());
        IMap<Integer, Employee> map = instance.getMap("default");
        map.addIndex("name", true);
        map.addIndex("city", true);
        testMultipleOrPredicates(map);
    }

    private void testMultipleOrPredicates(IMap<Integer, Employee> map) {
        for (int i = 0; i < 10; i++) {
            map.put(i, new Employee(i, "name" + i, "city" + i, i, true, i));
        }

        Collection<Employee> values;

        values = map.values(new SqlPredicate("name = 'name1' OR name = 'name2' OR name LIKE 'name3'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name = 'name1' OR name LIKE 'name2%' OR name LIKE 'name3'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name = 'name1' OR name LIKE 'name2%' OR name == 'name3'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name LIKE '%name1' OR name LIKE 'name2%' OR name LIKE '%name3%'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name == 'name1' OR name == 'name2' OR name = 'name3'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name = 'name1' OR name = 'name2' OR city LIKE 'city3'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name = 'name1' OR name LIKE 'name2%' OR city LIKE 'city3'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name = 'name1' OR name LIKE 'name2%' OR city == 'city3'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name LIKE '%name1' OR name LIKE 'name2%' OR city LIKE '%city3%'"));
        assertEquals(3, values.size());

        values = map.values(new SqlPredicate("name == 'name1' OR name == 'name2' OR city = 'city3'"));
        assertEquals(3, values.size());
    }

    @Test
    public void testSqlQueryUsing__KeyField() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance2.getMap(randomMapName());

        Object key = generateKeyOwnedBy(instance1);
        Object value = "value";
        map.put(key, value);

        Collection<Object> values = map.values(new SqlPredicate("__key = '" + key + "'"));
        assertEquals(1, values.size());
        assertEquals(value, values.iterator().next());
    }

    @Test
    public void testSqlQueryUsingNested__KeyField() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance.getMap(randomMapName());

        Object key = new CustomAttribute(12, 123L);
        Object value = "value";
        map.put(key, value);

        Collection<Object> values = map.values(new SqlPredicate("__key.age = 12 and __key.height = 123"));
        assertEquals(1, values.size());
        assertEquals(value, values.iterator().next());
    }

    @Test
    public void testSqlQueryUsingPortable__KeyField() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance.getMap(randomMapName());

        Object key = new ChildPortableObject(123L);
        Object value = "value";
        map.put(key, value);

        Collection<Object> values = map.values(new SqlPredicate("__key.timestamp = 123"));
        assertEquals(1, values.size());
        assertEquals(value, values.iterator().next());
    }

    @Test
    public void testQueryPortableObject_serial() {
        Config config = getConfig();
        testQueryUsingPortableObject(config, randomMapName());
    }

    @Test
    public void testQueryPortableObject_parallel() {
        Config config = getConfig();
        config.setProperty(GroupProperty.QUERY_PREDICATE_PARALLEL_EVALUATION.getName(), "true");
        testQueryUsingPortableObject(config, randomMapName());
    }

    @Test
    public void testQueryPortableObjectAndAlwaysCacheValues() {
        String name = randomMapName();
        Config config = getConfig();
        config.addMapConfig(new MapConfig(name).setCacheDeserializedValues(CacheDeserializedValues.ALWAYS));

        testQueryUsingPortableObject(config, name);
    }

    private void testQueryUsingPortableObject(Config config, String mapName) {
        addPortableFactories(config);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = instance2.getMap(mapName);

        Object key = generateKeyOwnedBy(instance1);
        map.put(key, new ParentPortableObject(1L));
        waitAllForSafeState(instance1, instance2);

        Collection<Object> values = map.values(new SqlPredicate("timestamp > 0"));
        assertEquals(1, values.size());
    }

    private void addPortableFactories(Config config) {
        config.getSerializationConfig()
                .addPortableFactory(1, new PortableFactory() {
                    @Override
                    public Portable create(int classId) {
                        return new GrandParentPortableObject(1L);
                    }
                }).addPortableFactory(2, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new ParentPortableObject(1L);
            }
        }).addPortableFactory(3, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                return new ChildPortableObject(1L);
            }
        });
    }

    @Test(expected = QueryException.class)
    public void testQueryPortableField() {
        Config config = getConfig();
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap(randomMapName());

        map.put(1, new GrandParentPortableObject(1, new ParentPortableObject(1L, new ChildPortableObject(1L))));
        Collection<Object> values = map.values(new SqlPredicate("child > 0"));
        values.size();
    }

    @Test
    public void testQueryUsingNestedPortableObject() {
        Config config = getConfig();
        testQueryUsingNestedPortableObject(config, randomMapName());
    }

    @Test
    public void testQueryUsingNestedPortableObjectWithIndex() {
        String name = randomMapName();
        Config config = getConfig();
        config.addMapConfig(new MapConfig(name)
                .addMapIndexConfig(new MapIndexConfig("child.timestamp", false))
                .addMapIndexConfig(new MapIndexConfig("child.child.timestamp", true)));

        testQueryUsingNestedPortableObject(config, name);
    }

    @Test
    public void testQueryPortableObjectWithIndex() {
        String name = randomMapName();
        Config config = getConfig();
        config.addMapConfig(new MapConfig(name)
                .addMapIndexConfig(new MapIndexConfig("timestamp", true)));

        testQueryUsingPortableObject(config, name);
    }

    @Test
    public void testQueryPortableObjectWithIndexAndAlwaysCacheValues() {
        String name = randomMapName();
        Config config = getConfig();
        config.addMapConfig(new MapConfig(name)
                .setCacheDeserializedValues(CacheDeserializedValues.ALWAYS)
                .addMapIndexConfig(new MapIndexConfig("timestamp", true)));

        testQueryUsingPortableObject(config, name);
    }

    private void testQueryUsingNestedPortableObject(Config config, String name) {
        addPortableFactories(config);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IMap<String, GrandParentPortableObject> map = instance2.getMap(name);

        String key = generateKeyOwnedBy(instance1);
        map.put(key, new GrandParentPortableObject(1, new ParentPortableObject(1L, new ChildPortableObject(1L))));
        waitAllForSafeState(instance1, instance2);

        Collection<GrandParentPortableObject> values = map.values(new SqlPredicate("child.timestamp > 0"));
        assertEquals(1, values.size());

        values = map.values(new SqlPredicate("child.child.timestamp > 0"));
        assertEquals(1, values.size());
    }
}
