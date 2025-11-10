/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.SampleTestObjects.PortableEmployee;
import com.hazelcast.query.SampleTestObjects.ValueType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
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

import static com.hazelcast.internal.util.RootCauseMatcher.rootCause;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_CLEANUP_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryAdvancedTest extends HazelcastTestSupport {

    protected void configureMap(String name, Config config) {
        // do nothing
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void testQueryOperationAreNotSentToLiteMembers() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        String mapName = randomMapName();
        Config fullConfig = getConfig();
        configureMap(mapName, fullConfig);
        HazelcastInstance fullMember = nodeFactory.newHazelcastInstance(fullConfig);
        Config liteConfig = getConfig().setLiteMember(true);
        configureMap(mapName, liteConfig);
        nodeFactory.newHazelcastInstance(liteConfig);

        assertClusterSizeEventually(2, fullMember);

        IMap<Integer, Integer> map = fullMember.getMap(mapName);
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
    public void testQueryWithTTL() {
        Config config = getConfig();
        // disable background expiration task to keep test safe from jitter
        config.setProperty(PROP_CLEANUP_ENABLED, "false");

        String mapName = "default";
        config.getMapConfig(mapName)
              .setTimeToLiveSeconds(3);
        configureMap(mapName, config);

        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<String, Employee> map = instance.getMap(mapName);
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        int passiveEmployees = 5;
        int activeEmployees = 5;
        int allEmployees = passiveEmployees + activeEmployees;

        final CountDownLatch latch = new CountDownLatch(allEmployees);
        map.addEntryListener((EntryExpiredListener) event -> latch.countDown(), false);

        for (int i = 0; i < activeEmployees; i++) {
            Employee employee = new Employee("activeEmployee" + i, 60, true, i);
            map.put("activeEmployee" + i, employee);
        }

        for (int i = 0; i < passiveEmployees; i++) {
            Employee employee = new Employee("passiveEmployee" + i, 60, false, i);
            map.put("passiveEmployee" + i, employee);
        }

        // check the query result before eviction
        final Collection values = map.values(Predicates.sql("active"));
        assertTrueEventually(() -> assertEquals(String.format("Expected %s results but got %s."
                        + " Number of evicted entries: %s.",
                activeEmployees, values.size(), allEmployees - latch.getCount()),
                activeEmployees, values.size()));

        // delete expire keys by explicitly calling `map#get`
        assertTrueEventually(() -> {
            for (int i = 0; i < activeEmployees; i++) {
                assertNull(map.get("activeEmployee" + i));
            }

            for (int i = 0; i < passiveEmployees; i++) {
                assertNull(map.get("passiveEmployee" + i));
            }
        });

        // wait until eviction is completed
        assertOpenEventually(latch);

        // check the query result after eviction
        assertEquals(0, map.values(Predicates.sql("active")).size());
    }

    @Test
    public void testTwoNodesWithPartialIndexes() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        for (int i = 0; i < 500; i++) {
            Employee employee = new Employee(i, "name" + i % 100, "city" + (i % 100), i % 60, ((i & 1) == 1), i);
            map.put(String.valueOf(i), employee);
        }
        assertClusterSize(2, instance1, instance2);

        map = instance2.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        Collection<Employee> entries = map.values(Predicates.sql("name='name3' and city='city3' and age > 2"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
        }

        entries = map.values(Predicates.sql("name LIKE '%name3' and city like '%city3' and age > 2"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
            assertTrue(employee.getAge() > 2);
        }

        entries = map.values(Predicates.sql("name LIKE '%name3%' and city like '%city30%'"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertTrue(employee.getName().startsWith("name3"));
            assertTrue(employee.getCity().startsWith("city3"));
        }
    }

    @Test
    public void testTwoNodesWithIndexes() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.HASH, "city");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        for (int i = 0; i < 5000; i++) {
            Employee employee = new Employee(i, "name" + i % 100, "city" + (i % 100), i % 60, ((i & 1) == 1), i);
            map.put(String.valueOf(i), employee);
        }
        assertClusterSize(2, instance1, instance2);

        map = instance2.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.HASH, "city");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        Collection<Employee> entries = map.values(Predicates.sql("name='name3' and city='city3' and age > 2"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
        }

        entries = map.values(Predicates.sql("name LIKE '%name3' and city like '%city3' and age > 2"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
            assertTrue(employee.getAge() > 2);
        }

        entries = map.values(Predicates.sql("name LIKE '%name3%' and city like '%city30%'"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertTrue(employee.getName().startsWith("name3"));
            assertTrue(employee.getCity().startsWith("city3"));
        }
    }

    @Test
    public void testOneMemberWithoutIndex() {
        Config config = getConfig();
        configureMap("employees", config);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testOneMemberWithIndex() {
        Config config = getConfig();
        configureMap("employees", config);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testOneMemberSQLWithoutIndex() {
        Config config = getConfig();
        configureMap("employees", config);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalSQLQueryTest(map);
        Set<Map.Entry<String, Employee>> entries = map.entrySet(Predicates.sql("active and age>23"));
        assertEquals(27, entries.size());
    }

    @Test
    public void testOneMemberSQLWithIndex() {
        Config config = getConfig();
        configureMap("employees", config);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");
        QueryBasicTest.doFunctionalSQLQueryTest(map);
    }

    @Test
    public void testTwoMembers() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testTwoMembersWithIndexes() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        QueryBasicTest.doFunctionalQueryTest(map);
        assertEquals(101, map.size());
        instance2.getLifecycleService().shutdown();
        assertEquals(101, map.size());

        Set<Map.Entry<String, Employee>> entries = map.entrySet(Predicates.sql("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(23, employee.getAge());
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown2() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        QueryBasicTest.doFunctionalQueryTest(map);
        assertEquals(101, map.size());
        instance1.getLifecycleService().shutdown();

        map = instance2.getMap("employees");
        assertEquals(101, map.size());
        Set<Map.Entry<String, Employee>> entries = map.entrySet(Predicates.sql("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(23, employee.getAge());
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown3() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");
        QueryBasicTest.doFunctionalQueryTest(map);

        assertEquals(101, map.size());
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        assertEquals(101, map.size());

        instance1.getLifecycleService().shutdown();
        map = instance2.getMap("employees");
        assertEquals(101, map.size());

        Set<Map.Entry<String, Employee>> entries = map.entrySet(Predicates.sql("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(23, employee.getAge());
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testSecondMemberAfterAddingIndexes() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        nodeFactory.newHazelcastInstance(config);
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testMapWithIndexAfterShutDown() {
        Config config = getConfig();
        String mapName = "default";
        config.getMapConfig(mapName).addIndexConfig(new IndexConfig(IndexType.HASH, "typeName"));
        configureMap(mapName, config);

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

        Collection typexValues = map.values(Predicates.sql("typeName = typex"));
        assertEquals(sampleSize2, typexValues.size());

        instances[1].shutdown();

        assertEquals(totalSize, map.size());
        assertTrueEventually(() -> {
            final Collection values = map.values(Predicates.sql("typeName = typex"));
            assertEquals(sampleSize2, values.size());
        });

        instances[2].shutdown();

        assertEquals(totalSize, map.size());
        assertTrueEventually(() -> {
            final Collection values = map.values(Predicates.sql("typeName = typex"));
            assertEquals(sampleSize2, values.size());
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
                Map<Integer, Employee> map = new HashMap<>();
                for (Integer key : keys) {
                    Employee emp = new Employee();
                    emp.setActive(true);
                    map.put(key, emp);
                }
                return map;
            }

            @Override
            public Set<Integer> loadAllKeys() {
                Set<Integer> set = new HashSet<>();
                for (int i = 0; i < size; i++) {
                    set.add(i);
                }
                return set;
            }
        });

        config.getMapConfig(name).setMapStoreConfig(mapStoreConfig);
        configureMap(name, config);
        HazelcastInstance instance = createHazelcastInstance(config);
        final IMap map = instance.getMap(name);
        assertTrueEventually(() -> {
            Collection values = map.values(Predicates.sql("active = true"));
            assertEquals(size, values.size());
        });
    }

    @Test
    // see: https://github.com/hazelcast/hazelcast/issues/3927
    public void testUnknownPortableField_notCausesQueryException_withoutIndex() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(666, classId -> new PortableEmployee());
        configureMap(mapName, config);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        IMap<Integer, PortableEmployee> map = hazelcastInstance.getMap(mapName);
        for (int i = 0; i < 5; i++) {
            map.put(i, new PortableEmployee(i, "name_" + i));
        }

        Collection values = map.values(Predicates.sql("notExist = name_0 OR a > 1"));
        assertEquals(3, values.size());
    }

    @Test
    // see: https://github.com/hazelcast/hazelcast/issues/3927
    public void testUnknownPortableField_notCausesQueryException_withIndex() {
        String mapName = "default";
        Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(666, classId -> new PortableEmployee());
        config.getMapConfig(mapName)
              .addIndexConfig(new IndexConfig(IndexType.HASH, "notExist"))
              .addIndexConfig(new IndexConfig(IndexType.HASH, "n"));
        configureMap(mapName, config);

        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        IMap<Integer, PortableEmployee> map = hazelcastInstance.getMap(mapName);
        for (int i = 0; i < 5; i++) {
            map.put(i, new PortableEmployee(i, "name_" + i));
        }

        Collection values = map.values(Predicates.sql("n = name_2 OR notExist = name_0"));
        assertEquals(1, values.size());
    }

    @Test
    public void testClassNotFoundErrorDelegatedToCallerOnQuery() {
        Config config = getConfig();
        configureMap("map", config);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);

        IMap<Integer, Integer> map = hazelcastInstance.getMap("map");

        map.put(1, 1);
        //A remote predicate can throw Error in case of Usercodeployment and missing sub classes
        //See the issue for actual problem https://github.com/hazelcast/hazelcast/issues/18052
        //We are throwing error to see if the error is delegated to the caller
        assertThatThrownBy(() -> map.values(new ErrorThrowingPredicate()))
                .isInstanceOf(HazelcastException.class)
                .cause().has(rootCause(NoClassDefFoundError.class));
    }
}
