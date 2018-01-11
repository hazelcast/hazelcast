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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.SampleTestObjects.PortableEmployee;
import com.hazelcast.query.SampleTestObjects.ValueType;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryAdvancedTest extends HazelcastTestSupport {

    @Test
    public void testQueryOperationAreNotSentToLiteMembers() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance fullMember = nodeFactory.newHazelcastInstance();
        HazelcastInstance liteMember = nodeFactory.newHazelcastInstance(new Config().setLiteMember(true));

        assertClusterSizeEventually(2, fullMember);

        IMap<Integer, Integer> map = fullMember.getMap(randomMapName());
        DeserializationCountingPredicate predicate = new DeserializationCountingPredicate();

        // initialize all partitions
        for (int i = 0; i < 5000; i++) {
            map.put(i, i);
        }

        map.values(predicate);
        assertEquals(0, predicate.serializationCount());
    }

    public static class DeserializationCountingPredicate implements Predicate, DataSerializable {
        private static final AtomicInteger counter = new AtomicInteger();

        @Override
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            counter.incrementAndGet();
        }

        int serializationCount() {
            return counter.get();
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testQueryWithTTL() throws Exception {
        Config config = getConfig();
        String mapName = "default";
        config.getMapConfig(mapName).setTimeToLiveSeconds(5);

        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<String, Employee> map = instance.getMap(mapName);
        map.addIndex("name", false);
        map.addIndex("age", false);
        map.addIndex("active", true);

        int passiveEmployees = 5;
        int activeEmployees = 5;
        int allEmployees = passiveEmployees + activeEmployees;

        final CountDownLatch latch = new CountDownLatch(allEmployees);
        map.addEntryListener(new EntryAdapter() {
            @Override
            public void entryEvicted(EntryEvent event) {
                latch.countDown();
            }
        }, false);

        for (int i = 0; i < activeEmployees; i++) {
            Employee employee = new Employee("activeEmployee" + i, 60, true, i);
            map.put("activeEmployee" + i, employee);
        }

        for (int i = 0; i < passiveEmployees; i++) {
            Employee employee = new Employee("passiveEmployee" + i, 60, false, i);
            map.put("passiveEmployee" + i, employee);
        }

        // check the query result before eviction
        Collection values = map.values(new SqlPredicate("active"));
        assertEquals(activeEmployees, values.size());

        // wait until eviction is completed
        assertOpenEventually(latch);

        // check the query result after eviction
        values = map.values(new SqlPredicate("active"));
        assertEquals(0, values.size());
    }

    @Test
    public void testTwoNodesWithPartialIndexes() throws Exception {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);

        for (int i = 0; i < 500; i++) {
            Employee employee = new Employee(i, "name" + i % 100, "city" + (i % 100), i % 60, ((i & 1) == 1), (double) i);
            map.put(String.valueOf(i), employee);
        }
        assertClusterSize(2, instance1, instance2);

        map = instance2.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);

        Collection<Employee> entries = map.values(new SqlPredicate("name='name3' and city='city3' and age > 2"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
        }

        entries = map.values(new SqlPredicate("name LIKE '%name3' and city like '%city3' and age > 2"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
            assertTrue(employee.getAge() > 2);
        }

        entries = map.values(new SqlPredicate("name LIKE '%name3%' and city like '%city30%'"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertTrue(employee.getName().startsWith("name3"));
            assertTrue(employee.getCity().startsWith("city3"));
        }
    }

    @Test
    public void testTwoNodesWithIndexes() throws Exception {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("city", false);
        map.addIndex("age", true);
        map.addIndex("active", false);

        for (int i = 0; i < 5000; i++) {
            Employee employee = new Employee(i, "name" + i % 100, "city" + (i % 100), i % 60, ((i & 1) == 1), (double) i);
            map.put(String.valueOf(i), employee);
        }
        assertClusterSize(2, instance1, instance2);

        map = instance2.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("city", false);
        map.addIndex("age", true);
        map.addIndex("active", false);

        Collection<Employee> entries = map.values(new SqlPredicate("name='name3' and city='city3' and age > 2"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
        }

        entries = map.values(new SqlPredicate("name LIKE '%name3' and city like '%city3' and age > 2"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
            assertTrue(employee.getAge() > 2);
        }

        entries = map.values(new SqlPredicate("name LIKE '%name3%' and city like '%city30%'"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertTrue(employee.getName().startsWith("name3"));
            assertTrue(employee.getCity().startsWith("city3"));
        }
    }

    @Test
    public void testOneMemberWithoutIndex() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testOneMemberWithIndex() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testOneMemberSQLWithoutIndex() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalSQLQueryTest(map);
        Set<Map.Entry<String, Employee>> entries = map.entrySet(new SqlPredicate("active and age>23"));
        assertEquals(27, entries.size());
    }

    @Test
    public void testOneMemberSQLWithIndex() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);
        QueryBasicTest.doFunctionalSQLQueryTest(map);
    }

    @Test
    public void testTwoMembers() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testTwoMembersWithIndexes() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);

        QueryBasicTest.doFunctionalQueryTest(map);
        assertEquals(101, map.size());
        instance2.getLifecycleService().shutdown();
        assertEquals(101, map.size());

        Set<Map.Entry<String, Employee>> entries = map.entrySet(new SqlPredicate("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(employee.getAge(), 23);
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown2() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);

        QueryBasicTest.doFunctionalQueryTest(map);
        assertEquals(101, map.size());
        instance1.getLifecycleService().shutdown();

        map = instance2.getMap("employees");
        assertEquals(101, map.size());
        Set<Map.Entry<String, Employee>> entries = map.entrySet(new SqlPredicate("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(employee.getAge(), 23);
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown3() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);
        QueryBasicTest.doFunctionalQueryTest(map);

        assertEquals(101, map.size());
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        assertEquals(101, map.size());

        instance1.getLifecycleService().shutdown();
        map = instance2.getMap("employees");
        assertEquals(101, map.size());

        Set<Map.Entry<String, Employee>> entries = map.entrySet(new SqlPredicate("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(employee.getAge(), 23);
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testSecondMemberAfterAddingIndexes() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex("name", false);
        map.addIndex("age", true);
        map.addIndex("active", false);

        nodeFactory.newHazelcastInstance(config);
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testMapWithIndexAfterShutDown() {
        Config config = getConfig();
        String mapName = "default";
        config.getMapConfig(mapName).addMapIndexConfig(new MapIndexConfig("typeName", false));

        HazelcastInstance[] instances = createHazelcastInstanceFactory(3).newInstances(config);

        final IMap<Integer, ValueType> map = instances[0].getMap(mapName);
        final int sampleSize1 = 100;
        final int sampleSize2 = 30;
        int totalSize = sampleSize1 + sampleSize2;

        for (int i = 0; i < sampleSize1; i++) {
            map.put(i, new ValueType("type" + i));
        }

        for (int i = sampleSize1; i < totalSize; i++) {
            map.put(i, new ValueType("typex"));
        }

        Collection typexValues = map.values(new SqlPredicate("typeName = typex"));
        assertEquals(sampleSize2, typexValues.size());

        instances[1].shutdown();

        assertEquals(totalSize, map.size());
        assertTrueEventually(new AssertTask() {
            public void run() {
                final Collection values = map.values(new SqlPredicate("typeName = typex"));
                assertEquals(sampleSize2, values.size());
            }
        });

        instances[2].shutdown();

        assertEquals(totalSize, map.size());
        assertTrueEventually(new AssertTask() {
            public void run() {
                final Collection values = map.values(new SqlPredicate("typeName = typex"));
                assertEquals(sampleSize2, values.size());
            }
        });
    }

    // issue 1404 "to be fixed by issue 1404"
    @Test
    public void testQueryAfterInitialLoad() {
        final int size = 100;
        String name = "default";
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Employee>() {
            @Override
            public Map<Integer, Employee> loadAll(Collection<Integer> keys) {
                Map<Integer, Employee> map = new HashMap<Integer, Employee>();
                for (Integer key : keys) {
                    Employee emp = new Employee();
                    emp.setActive(true);
                    map.put(key, emp);
                }
                return map;
            }

            @Override
            public Set<Integer> loadAllKeys() {
                Set<Integer> set = new HashSet<Integer>();
                for (int i = 0; i < size; i++) {
                    set.add(i);
                }
                return set;
            }
        });

        config.getMapConfig(name).setMapStoreConfig(mapStoreConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap map = instance.getMap(name);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection values = map.values(new SqlPredicate("active = true"));
                assertEquals(size, values.size());
            }
        });
    }

    @Test
    // see: https://github.com/hazelcast/hazelcast/issues/3927
    public void testUnknownPortableField_notCausesQueryException_withoutIndex() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(666, new PortableFactory() {
            public Portable create(int classId) {
                return new PortableEmployee();
            }
        });
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        IMap<Integer, PortableEmployee> map = hazelcastInstance.getMap(mapName);
        for (int i = 0; i < 5; i++) {
            map.put(i, new PortableEmployee(i, "name_" + i));
        }

        Collection values = map.values(new SqlPredicate("notExist = name_0 OR a > 1"));
        assertEquals(3, values.size());
    }

    @Test
    // see: https://github.com/hazelcast/hazelcast/issues/3927
    public void testUnknownPortableField_notCausesQueryException_withIndex() {
        String mapName = "default";
        Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(666, new PortableFactory() {
            public Portable create(int classId) {
                return new PortableEmployee();
            }
        });
        config.getMapConfig(mapName)
                .addMapIndexConfig(new MapIndexConfig("notExist", false))
                .addMapIndexConfig(new MapIndexConfig("n", false));

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        IMap<Integer, PortableEmployee> map = hazelcastInstance.getMap(mapName);
        for (int i = 0; i < 5; i++) {
            map.put(i, new PortableEmployee(i, "name_" + i));
        }

        Collection values = map.values(new SqlPredicate("n = name_2 OR notExist = name_0"));
        assertEquals(1, values.size());
    }
}
