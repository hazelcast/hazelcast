/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapFetchIndexOperationTest extends HazelcastTestSupport {
    private static final String mapName = "map1";
    private static final String orderedIndexName = "index_age_sorted";

    private static HazelcastInstance instance1;

    private static IMap<String, Person> map;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = smallInstanceConfig();
        instance1 = factory.newHazelcastInstance(config);

        map = instance1.getMap(mapName);

        map.addIndex(new IndexConfig(IndexType.SORTED, "age").setName(orderedIndexName));

        List<Person> people = new ArrayList<>(
                Arrays.asList(
                        new Person("person1", 45),
                        new Person("person2", 39),
                        new Person("person3", 60),
                        new Person("person4", 45),
                        new Person("person5", 43)
                )
        );

        insertIntoMap(map, people);
    }

    @Test
    public void testMultipleRanges() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance1);

        IndexIterationPointer[] pointers = new IndexIterationPointer[2];
        pointers[0] = new IndexIterationPointer(30, true, 40, true, false);
        pointers[1] = new IndexIterationPointer(50, true, 60, true, false);

        MapOperationProvider operationProvider = getOperationProvider();
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 5);

        Address address = instance1.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance1);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();


        assertContainsSorted(result, Arrays.asList(
                new Person("person2", 39),
                new Person("person3", 60)
        ));
    }

    @Test
    public void whenMultipleObjectsHasSameValue_thenOperationCanReturnMoreThanSizeHint() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance1);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = new IndexIterationPointer(30, true, 60, true, false);

        MapOperationProvider operationProvider = getOperationProvider();
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 3);

        Address address = instance1.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance1);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();


        assertContainsSorted(result, Arrays.asList(
                new Person("person2", 39),
                new Person("person5", 43),
                new Person("person4", 45),
                new Person("person1", 45)
        ));
    }

    private MapOperationProvider getOperationProvider()  {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapServiceContext mapServiceContext = ((MapService) mapProxy.getService()).getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapProxy.getName());
    }

    private static PartitionIdSet getLocalPartitions(HazelcastInstance member) {
        PartitionService partitionService = member.getPartitionService();

        PartitionIdSet res = new PartitionIdSet(partitionService.getPartitions().size());

        for (Partition partition : partitionService.getPartitions()) {
            if (partition.getOwner().localMember()) {
                res.add(partition.getPartitionId());
            }
        }

        return res;
    }

    private static void insertIntoMap(IMap<String, Person> map, List<Person> peopleList) {
        peopleList.forEach(p -> map.put(p.getName(), p));
    }

    private static void assertContainsSorted(MapFetchIndexOperationResult result, List<Person> orderedPeopleList) {
        List<QueryableEntry> entries = result.getResult();
        assertEquals(entries.size(), orderedPeopleList.size());

        Iterator<QueryableEntry> entriesIterator = entries.iterator();
        Iterator<Person> expectedIterator = orderedPeopleList.iterator();

        while (entriesIterator.hasNext()) {
            QueryableEntry entry = entriesIterator.next();
            Person expected = expectedIterator.next();

            assertEquals(expected.getName(), entry.getKey());
            assertEquals(expected, entry.getValue());
        }
    }

    static class Person implements Serializable {
        private String name;
        private int age;

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return this.name;
        }

        public int getAge() {
            return this.age;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{"
                    + "name='" + name + '\''
                    + ", age=" + age
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Person person = (Person) o;
            return age == person.age && name.equals(person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }
    }
}
