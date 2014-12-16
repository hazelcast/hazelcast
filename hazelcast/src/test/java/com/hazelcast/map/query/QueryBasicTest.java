package com.hazelcast.map.query;


import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.PortableTest.ChildPortableObject;
import com.hazelcast.nio.serialization.PortableTest.GrandParentPortableObject;
import com.hazelcast.nio.serialization.PortableTest.ParentPortableObject;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.query.SampleObjects.State;
import com.hazelcast.query.SampleObjects.Value;
import com.hazelcast.query.SampleObjects.ValueType;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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
@Category(QuickTest.class)
public class QueryBasicTest extends HazelcastTestSupport {

    @Test(timeout = 1000 * 60)
    public void testInPredicateWithEmptyArray() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
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

    @Test(timeout = 1000 * 60)
    public void issue393SqlIn() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("name", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i);
            map.put("" + i, v);
        }
        Predicate predicate = new SqlPredicate("name IN ('name0', 'name2')");
        Collection<SampleObjects.Value> values = map.values(predicate);
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

    @Test(timeout = 1000 * 60)
    public void issue393SqlInInteger() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("index", false);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i, new ValueType("type" + i), i);
            map.put("" + i, v);
        }
        Predicate predicate = new SqlPredicate("index IN (0, 2)");
        Collection<SampleObjects.Value> values = map.values(predicate);
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

    @Test(timeout = 1000 * 60)
    public void testInPredicate() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.ValueType> map = instance.getMap("testIteratorContract");
        map.put("1", new ValueType("one"));
        map.put("2", new ValueType("two"));
        map.put("3", new ValueType("three"));
        map.put("4", new ValueType("four"));
        map.put("5", new ValueType("five"));
        map.put("6", new ValueType("six"));
        map.put("7", new ValueType("seven"));
        Predicate predicate = new SqlPredicate("typeName in ('one','two')");
        for (int i = 0; i < 10; i++) {
            Collection<SampleObjects.ValueType> values = map.values(predicate);
            assertEquals(2, values.size());
        }
    }

    @Test(timeout = 1000 * 60)
    public void testInstanceofPredicate() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, Object> map = instance.getMap("testInstanceofPredicate");

        LinkedList linkedList = new LinkedList();

        Predicate linkedListPredicate = Predicates.instanceOf(LinkedList.class);
        map.put("1", "somestring");
        map.put("2", new ArrayList());
        map.put("3", linkedList);

        Collection<Object> values = map.values(linkedListPredicate);
        assertEquals(1, values.size());
        assertTrue(values.contains(linkedList));
    }

    @Test(timeout = 1000 * 60)
    public void testIteratorContract() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.ValueType> map = instance.getMap("testIteratorContract");
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


    @Test(timeout = 1000 * 60)
    public void issue393Fail() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("qwe", true);
        Value v = new Value("name");
        try {
            map.put("0", v);
            fail();
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains("There is no suitable accessor for 'qwe'"));
        }
    }

    @Test(timeout = 1000 * 60)
    public void negativeDouble() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Employee> map = instance.getMap("default");
        map.addIndex("salary", false);
        map.put("" + 4, new Employee(1, "default", 1, true, -70D));
        map.put("" + 3, new Employee(1, "default", 1, true, -60D));
        map.put("" + 1, new Employee(1, "default", 1, true, -10D));
        map.put("" + 2, new Employee(2, "default", 2, true, 10D));
        Predicate predicate = new SqlPredicate("salary >= -60");
        Collection<SampleObjects.Employee> values = map.values(predicate);
        assertEquals(3, values.size());
        predicate = new SqlPredicate("salary between -20 and 20");
        values = map.values(predicate);
        assertEquals(2, values.size());
    }

    @Test(timeout = 1000 * 60)
    public void issue393SqlEq() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("name", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i);
            map.put("" + i, v);
        }
        Predicate predicate = new SqlPredicate("name='name0'");
        Collection<SampleObjects.Value> values = map.values(predicate);
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

    @Test(timeout = 1000 * 60)
    public void issue393() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<String, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("name", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value("name" + i);
            map.put("" + i, v);
        }
        Predicate predicate = new PredicateBuilder().getEntryObject().get("name").in("name0", "name2");
        Collection<SampleObjects.Value> values = map.values(predicate);
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

    @Test(timeout = 1000 * 60)
    public void testWithDashInTheNameAndSqlPredicate() {
        HazelcastInstance h1 = createHazelcastInstance();
        IMap<String, SampleObjects.Employee> map = h1.getMap("employee");
        Employee toto = new Employee("toto", 23, true, 165765.0);
        map.put("1", toto);
        Employee toto2 = new Employee("toto-super+hero", 23, true, 165765.0);
        map.put("2", toto2);
        //Works well
        Set<Map.Entry<String, SampleObjects.Employee>> entries = map.entrySet(new SqlPredicate("name='toto-super+hero'"));
        assertTrue(entries.size() > 0);
        for (Map.Entry<String, SampleObjects.Employee> entry : entries) {
            Employee e = entry.getValue();
            assertEquals(e, toto2);
        }
    }

    @Test(timeout = 1000 * 60)
    public void queryWithThis() {
        HazelcastInstance instance = createHazelcastInstance();
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
    @Test(timeout = 1000 * 60)
    public void testPredicateWithEntryKeyObject() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map = instance.getMap("testPredicateWithEntryKeyObject");
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
    @Test(timeout = 1000 * 60)
    public void testPredicateStringAttribute() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map = instance.getMap("testPredicateStringWithString");
        testPredicateStringAttribute(map);
    }

    /**
     * Github issues 98 and 131
     */
    @Test(timeout = 1000 * 60)
    public void testPredicateStringAttributesWithIndex() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map = instance.getMap("testPredicateStringWithStringIndex");
        map.addIndex("name", false);
        testPredicateStringAttribute(map);
    }

    private void testPredicateStringAttribute(IMap map) {
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

    @Test(timeout = 1000 * 60)
    public void testPredicateDateAttribute() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map = instance.getMap("testPredicateDateAttribute");
        testPredicateDateAttribute(map);
    }

    @Test(timeout = 1000 * 60)
    public void testPredicateDateAttributeWithIndex() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map = instance.getMap("testPredicateDateAttribute");
        map.addIndex("this", true);
        testPredicateDateAttribute(map);
    }

    private void testPredicateDateAttribute(IMap map) {
        Calendar cal = Calendar.getInstance();
        cal.set(2012, 5, 5);
        map.put(1, cal.getTime());
        cal.set(2011, 10, 10);
        map.put(2, cal.getTime());
        cal.set(2011, 1, 1);
        map.put(3, cal.getTime());
        cal.set(2010, 8, 5);
        map.put(4, cal.getTime());
        cal.set(2000, 5, 5);
        map.put(5, cal.getTime());
        cal.set(2011, 0, 1);
        assertEquals(3, map.values(new PredicateBuilder().getEntryObject().get("this").greaterThan(cal.getTime())).size());
        assertEquals(3, map.values(new SqlPredicate("this > 'Sat Jan 01 11:43:05 EET 2011'")).size());
        assertEquals(2, map.values(new PredicateBuilder().getEntryObject().get("this").lessThan(cal.getTime())).size());
        assertEquals(2, map.values(new SqlPredicate("this < 'Sat Jan 01 11:43:05 EET 2011'")).size());
        cal.set(2003, 10, 10);
        Date d1 = cal.getTime();
        cal.set(2012, 1, 10);
        Date d2 = cal.getTime();
        assertEquals(3, map.values(new PredicateBuilder().getEntryObject().get("this").between(d1, d2)).size());
        assertEquals(3, map.values(new SqlPredicate("this between 'Mon Nov 10 11:43:05 EET 2003'" +
                " and 'Fri Feb 10 11:43:05 EET 2012'")).size());
    }

    @Test(timeout = 1000 * 60)
    public void testPredicateEnumAttribute() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map = instance.getMap("testPredicateEnumAttribute");
        testPredicateEnumAttribute(map);
    }

    @Test(timeout = 1000 * 60)
    public void testPredicateEnumAttributeWithIndex() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map = instance.getMap("testPredicateEnumAttribute");
        map.addIndex("this", true);
        testPredicateDateAttribute(map);
    }

    private void testPredicateEnumAttribute(IMap map) {
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


    public enum NodeType {
        MEMBER(1),
        LITE_MEMBER(2),
        JAVA_CLIENT(3),
        CSHARP_CLIENT(4);

        private int value;

        private NodeType(int type) {
            this.value = type;
        }

        public int getValue() {
            return value;
        }

        public static NodeType create(int value) {
            switch (value) {
                case 1:
                    return MEMBER;
                case 2:
                    return LITE_MEMBER;
                case 3:
                    return JAVA_CLIENT;
                case 4:
                    return CSHARP_CLIENT;
                default:
                    return null;
            }
        }
    }

    @Test(timeout = 1000 * 60)
    public void testPredicateCustomAttribute() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap map = instance.getMap("testPredicateCustomAttribute");

        CustomAttribute attribute = new CustomAttribute(78, 145);
        CustomObject object = new CustomObject("name1", UuidUtil.buildRandomUUID(), attribute);
        map.put(1, object);

        CustomObject object2 = new CustomObject("name2", UuidUtil.buildRandomUUID(), attribute);
        map.put(2, object2);

        assertEquals(object, map.values(new PredicateBuilder().getEntryObject().get("uuid").equal(object.getUuid())).iterator().next());
        assertEquals(2, map.values(new PredicateBuilder().getEntryObject().get("attribute").equal(attribute)).size());

        assertEquals(object2, map.values(new PredicateBuilder().getEntryObject().get("uuid").in(object2.getUuid())).iterator().next());
        assertEquals(2, map.values(new PredicateBuilder().getEntryObject().get("attribute").in(attribute)).size());
    }

    public static void doFunctionalSQLQueryTest(IMap imap) {
        imap.put("1", new Employee("joe", 33, false, 14.56));
        imap.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        Set<Map.Entry> entries = imap.entrySet();
        assertEquals(102, entries.size());
        int itCount = 0;
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            itCount++;
        }
        assertEquals(102, itCount);
        entries = imap.entrySet(new SqlPredicate("active=true and age=23"));
        assertEquals(3, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.remove("2");
        entries = imap.entrySet(new SqlPredicate("active=true and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        entries = imap.entrySet(new SqlPredicate("age!=33"));
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertTrue(c.getAge() != 33);
        }
        entries = imap.entrySet(new SqlPredicate("active!=false"));
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertTrue(c.isActive());
        }
    }

    public static void doFunctionalQueryTest(IMap imap) {
        imap.put("1", new Employee("joe", 33, false, 14.56));
        imap.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i & 1) == 1), Double.valueOf(i)));
        }
        Set<Map.Entry> entries = imap.entrySet();
        assertEquals(102, entries.size());
        int itCount = 0;
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            itCount++;
        }
        assertEquals(102, itCount);
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));
        entries = imap.entrySet(predicate);
//        assertEquals(3, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.remove("2");
        entries = imap.entrySet(predicate);
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        entries = imap.entrySet(new SqlPredicate(" (age >= " + 30 + ") AND (age <= " + 40 + ")"));
        assertEquals(23, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertTrue(c.getAge() >= 30);
            assertTrue(c.getAge() <= 40);
        }
    }

    @Test(timeout = 1000 * 60)
    public void testInvalidSqlPredicate() {
        Config cfg = new Config();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);
        IMap map = instance.getMap("employee");
        map.put(1, new Employee("e", 1, false, 0));
        map.put(2, new Employee("e2", 1, false, 0));
        try {
            map.values(new SqlPredicate("invalid_sql"));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("There is no suitable accessor for 'invalid_sql'"));
        }
        try {
            map.values(new SqlPredicate("invalid sql"));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Invalid SQL: [invalid sql]"));
        }
        try {
            map.values(new SqlPredicate("invalid and sql"));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("There is no suitable accessor for 'invalid'"));
        }
        try {
            map.values(new SqlPredicate("invalid sql and"));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("There is no suitable accessor for 'invalid'"));
        }
        try {
            map.values(new SqlPredicate(""));
            fail("Should fail because of invalid SQL!");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Invalid SQL: []"));
        }
        assertEquals(2, map.values(new SqlPredicate("age=1 and name like 'e%'")).size());
    }

    @Test(timeout = 1000 * 60)
    public void testIndexingEnumAttributeIssue597() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("state", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value(i % 2 == 0 ? State.STATE1 : State.STATE2, new ValueType(), i);
            map.put(i, v);
        }
        Predicate predicate = new PredicateBuilder().getEntryObject().get("state").equal(State.STATE1);
        Collection<SampleObjects.Value> values = map.values(predicate);
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
    @Test(timeout = 1000 * 60)
    public void testIndexingEnumAttributeWithSqlIssue597() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, SampleObjects.Value> map = instance.getMap("default");
        map.addIndex("state", true);
        for (int i = 0; i < 4; i++) {
            Value v = new Value(i % 2 == 0 ? State.STATE1 : State.STATE2, new ValueType(), i);
            map.put(i, v);
        }

        Collection<SampleObjects.Value> values = map.values(new SqlPredicate("state = 'STATE1'"));
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

    @Test(timeout = 1000 * 60)
    public void testMultipleOrPredicatesIssue885WithoutIndex() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(new Config());
        factory.newHazelcastInstance(new Config());
        IMap<Integer, SampleObjects.Employee> map = instance.getMap("default");
        testMultipleOrPredicates(map);
    }

    @Test(timeout = 1000 * 60)
    public void testMultipleOrPredicatesIssue885WithIndex() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(new Config());
        factory.newHazelcastInstance(new Config());
        IMap<Integer, SampleObjects.Employee> map = instance.getMap("default");
        map.addIndex("name", true);
        testMultipleOrPredicates(map);
    }

    @Test(timeout = 1000 * 60)
    public void testMultipleOrPredicatesIssue885WithDoubleIndex() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(new Config());
        factory.newHazelcastInstance(new Config());
        IMap<Integer, SampleObjects.Employee> map = instance.getMap("default");
        map.addIndex("name", true);
        map.addIndex("city", true);
        testMultipleOrPredicates(map);
    }

    private void testMultipleOrPredicates(IMap<Integer, SampleObjects.Employee> map) {
        for (int i = 0; i < 10; i++) {
            map.put(i, new Employee(i, "name" + i, "city" + i, i, true, i));
        }

        Collection<SampleObjects.Employee> values;

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
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        IMap<Object, Object> map = hz2.getMap(randomMapName());

        Object key = generateKeyOwnedBy(hz1);
        Object value = "value";
        map.put(key, value);

        Collection<Object> values = map.values(new SqlPredicate("__key = '" + key + "'"));
        assertEquals(1, values.size());
        assertEquals(value, values.iterator().next());
    }

    @Test
    public void testSqlQueryUsingNested__KeyField() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        IMap<Object, Object> map = hz2.getMap(randomMapName());

        Object key = new CustomAttribute(12, 123L);
        Object value = "value";
        map.put(key, value);

        Collection<Object> values = map.values(new SqlPredicate("__key.age = 12 and __key.height = 123"));
        assertEquals(1, values.size());
        assertEquals(value, values.iterator().next());
    }

    @Test
    public void testSqlQueryUsingPortable__KeyField() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        IMap<Object, Object> map = hz2.getMap(randomMapName());

        Object key = new ChildPortableObject(123L);
        Object value = "value";
        map.put(key, value);

        Collection<Object> values = map.values(new SqlPredicate("__key.timestamp = 123"));
        assertEquals(1, values.size());
        assertEquals(value, values.iterator().next());
    }

    @Test
    public void testQueryPortableObject() {
        testQueryUsingPortableObject(new Config(), randomMapName());
    }

    @Test
    public void testQueryPortableObjectAndOptimizeQueries() {
        String name = randomMapName();
        Config config = new Config();
        config.addMapConfig(new MapConfig(name).setOptimizeQueries(true));

        testQueryUsingPortableObject(config, name);
    }

    private void testQueryUsingPortableObject(Config config, String mapName) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = hz2.getMap(mapName);

        Object key = generateKeyOwnedBy(hz1);
        map.put(key, new ParentPortableObject(1L));
        waitAllForSafeState(hz1, hz2);

        Collection<Object> values = map.values(new SqlPredicate("timestamp > 0"));
        assertEquals(1, values.size());
    }

    @Test(expected = QueryException.class)
    public void testQueryPortableField() {
        HazelcastInstance hz = createHazelcastInstance();
        IMap<Object, Object> map = hz.getMap(randomMapName());

        map.put(1, new GrandParentPortableObject(1, new ParentPortableObject(1L, new ChildPortableObject(1L))));
        Collection<Object> values = map.values(new SqlPredicate("child > 0"));
        values.size();
    }

    @Test
    public void testQueryUsingNestedPortableObject() {
        testQueryUsingNestedPortableObject(new Config(), randomMapName());
    }

    @Test
    public void testQueryUsingNestedPortableObjectWithIndex() {
        String name = randomMapName();
        Config config = new Config();
        config.addMapConfig(new MapConfig(name).addMapIndexConfig(new MapIndexConfig("child.timestamp", false))
                .addMapIndexConfig(new MapIndexConfig("child.child.timestamp", true)));

        testQueryUsingNestedPortableObject(config, name);
    }

    @Test
    public void testQueryPortableObjectWithIndex() {
        String name = randomMapName();
        Config config = new Config();
        MapConfig mapConfig = new MapConfig(name)
                .addMapIndexConfig(new MapIndexConfig("timestamp", true));
        config.addMapConfig(mapConfig);

        testQueryUsingPortableObject(config, name);
    }

    @Test
    public void testQueryPortableObjectWithIndexAndOptimizeQueries() {
        String name = randomMapName();
        Config config = new Config();
        MapConfig mapConfig = new MapConfig(name)
                .setOptimizeQueries(true)
                .addMapIndexConfig(new MapIndexConfig("timestamp", true));
        config.addMapConfig(mapConfig);

        testQueryUsingPortableObject(config, name);
    }

    private void testQueryUsingNestedPortableObject(Config config, String name) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = hz2.getMap(name);

        Object key = generateKeyOwnedBy(hz1);
        map.put(key, new GrandParentPortableObject(1, new ParentPortableObject(1L, new ChildPortableObject(1L))));
        waitAllForSafeState(hz1, hz2);

        Collection<Object> values = map.values(new SqlPredicate("child.timestamp > 0"));
        assertEquals(1, values.size());

        values = map.values(new SqlPredicate("child.child.timestamp > 0"));
        assertEquals(1, values.size());
    }

}
