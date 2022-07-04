/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MissingPartitionException;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapFetchIndexOperationTest extends HazelcastTestSupport {
    private static final String mapName = "map1";
    private static final String orderedIndexName = "index_age_sorted";
    private static final String compositeOrderedIndexName = "index_age_name_sorted";
    private static final String hashIndexName = "index_age_hash";

    private HazelcastInstance instance;
    private Config config;

    private IMap<String, Person> map;

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);

        config = smallInstanceConfig();
        instance = factory.newHazelcastInstance(config);

        map = instance.getMap(mapName);

        map.addIndex(new IndexConfig(IndexType.SORTED, "age").setName(orderedIndexName));
        map.addIndex(new IndexConfig(IndexType.HASH, "age").setName(hashIndexName));
        map.addIndex(new IndexConfig(IndexType.SORTED, "age", "department").setName(compositeOrderedIndexName));

        List<Person> people = new ArrayList<>(
                Arrays.asList(
                        new Person("person1", 45 , "Dep1"),
                        new Person("person2", 39, "Dep1"),
                        new Person("person3", 60, "Dep1"),
                        new Person("person4", 45, "Dep2"),
                        new Person("person5", 43, "Dep2"),
                        new Person("person6", null, "Dep1"),
                        new Person("person7", null, null),
                        new Person("person8", 79, null),
                        new Person("person9", 45, "Dep3"),
                        new Person("person10", 45, "Dep4"),
                        new Person("person11", 45, "Dep5")
                )
        );

        insertIntoMap(map, people);
    }

    @After
    public void destroy() {
        instance.shutdown();
    }

    @Test
    public void testRange() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = IndexIterationPointer.create(30, true, 60, false, false, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 7);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result, Arrays.asList(
                new Person("person2", 39, "Dep1"),
                new Person("person5", 43, "Dep2"),
                new Person("person1", 45, "Dep1"),
                new Person("person4", 45, "Dep2"),
                new Person("person9", 45, "Dep3"),
                new Person("person10", 45, "Dep4"),
                new Person("person11", 45, "Dep5")
        ));
    }

    @Test
    public void testOneSideRange() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = IndexIterationPointer.create(null, true, 60, false, true, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 10);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result, Arrays.asList(
                new Person("person11", 45, "Dep5"),
                new Person("person10", 45, "Dep4"),
                new Person("person9", 45, "Dep3"),
                new Person("person4", 45, "Dep2"),
                new Person("person1", 45, "Dep1"),
                new Person("person5", 43, "Dep2"),
                new Person("person2", 39, "Dep1")
        ));
    }

    @Test
    public void testRangeReverse() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = IndexIterationPointer.create(30, true, 60, false, true, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 10);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result, Arrays.asList(
                new Person("person11", 45, "Dep5"),
                new Person("person10", 45, "Dep4"),
                new Person("person9", 45, "Dep3"),
                new Person("person4", 45, "Dep2"),
                new Person("person1", 45, "Dep1"),
                new Person("person5", 43, "Dep2"),
                new Person("person2", 39, "Dep1")
        ));
    }

    @Test
    public void testRangeComposite() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = IndexIterationPointer.create(
                new CompositeValue(new Comparable[] {30, CompositeValue.NEGATIVE_INFINITY}),
                true,
                new CompositeValue(new Comparable[] {CompositeValue.POSITIVE_INFINITY, CompositeValue.POSITIVE_INFINITY}),
                true,
                false,
                null
        );

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, compositeOrderedIndexName, pointers, partitions, 10);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result, Arrays.asList(
                new Person("person2", 39, "Dep1"),
                new Person("person5", 43, "Dep2"),
                new Person("person1", 45 , "Dep1"),
                new Person("person4", 45, "Dep2"),
                new Person("person9", 45, "Dep3"),
                new Person("person10", 45, "Dep4"),
                new Person("person11", 45, "Dep5"),
                new Person("person3", 60, "Dep1"),
                new Person("person8", 79, null)
        ));
    }

    @Test
    public void testFullScan() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = IndexIterationPointer.create(null, true, null, true, false, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 20);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result, Arrays.asList(
                new Person("person6", null, "Dep1"),
                new Person("person7", null, null),
                new Person("person2", 39, "Dep1"),
                new Person("person5", 43, "Dep2"),
                new Person("person1", 45, "Dep1"),
                new Person("person4", 45, "Dep2"),
                new Person("person9", 45, "Dep3"),
                new Person("person10", 45, "Dep4"),
                new Person("person11", 45, "Dep5"),
                new Person("person3", 60, "Dep1"),
                new Person("person8", 79, null)
        ));
    }

    @Test
    public void testMultipleLookups() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[3];
        pointers[0] = IndexIterationPointer.create(30, true, 30, true, false, null);
        pointers[1] = IndexIterationPointer.create(39, true, 39, true, false, null);
        pointers[2] = IndexIterationPointer.create(45, true, 45, true, false, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, hashIndexName, pointers, partitions, 10);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResult(result, Arrays.asList(
                new Person("person2", 39, "Dep1"),
                new Person("person1", 45, "Dep1"),
                new Person("person4", 45, "Dep2"),
                new Person("person9", 45, "Dep3"),
                new Person("person10", 45, "Dep4"),
                new Person("person11", 45, "Dep5")
        ));
    }

    @Test
    public void testMultipleRanges() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[2];
        pointers[0] = IndexIterationPointer.create(30, true, 40, true, false, null);
        pointers[1] = IndexIterationPointer.create(50, true, 60, true, false, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 5);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result, Arrays.asList(
                new Person("person2", 39, "Dep1"),
                new Person("person3", 60, "Dep1")
        ));
    }

    @Test
    public void whenSizeLimitIsSmall_thenFetchInMultipleCalls() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[2];
        pointers[0] = IndexIterationPointer.create(30, true, 50, false, false, null);
        pointers[1] = IndexIterationPointer.create(50, true, 60, true, false, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 5);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result,
                Arrays.asList(
                        new Person("person2", 39, "Dep1"),
                        new Person("person5", 43, "Dep2"),
                        new Person("person1", 45 , "Dep1"),
                        new Person("person4", 45, "Dep2"),
                        new Person("person9", 45, "Dep3")
                ));

        // First pointer is not done yet
        assertEquals(2, result.getPointers().length);

        operation = operationProvider.createFetchIndexOperation(
                mapName, orderedIndexName, result.getPointers(), partitions, 5);

        result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result,
                Arrays.asList(
                    new Person("person10", 45, "Dep4"),
                    new Person("person11", 45, "Dep5"),
                    new Person("person3", 60, "Dep1")
                ));

        assertEquals(0, result.getPointers().length);
    }

    @Test
    public void whenSizeLimitIsSmall_thenFetchInMultipleCalls_reverse() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[2];
        pointers[0] = IndexIterationPointer.create(50, true, 60, true, true, null);
        pointers[1] = IndexIterationPointer.create(30, true, 50, false, true, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 5);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result,
                Arrays.asList(
                        new Person("person3", 60, "Dep1"),
                        new Person("person11", 45, "Dep5"),
                        new Person("person10", 45, "Dep4"),
                        new Person("person9", 45, "Dep3"),
                        new Person("person4", 45, "Dep2")
                ));

        // First pointer is done in reverse case
        assertEquals(1, result.getPointers().length);

        operation = operationProvider.createFetchIndexOperation(
                mapName, orderedIndexName, result.getPointers(), partitions, 5);

        result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result,
                Arrays.asList(
                        new Person("person1", 45 , "Dep1"),
                        new Person("person5", 43, "Dep2"),
                        new Person("person2", 39, "Dep1")
                ));

        assertEquals(0, result.getPointers().length);
    }

    // This test has different requirements, therefore depends on local variables.
    // Before and After actions are dismissed.
    @Test
    public void testMigration() {
        HazelcastInstance instance = new CustomTestInstanceFactory().newHazelcastInstance(config);
        PartitionIdSet partitions = getLocalPartitions(instance);

        List<Person> people = new ArrayList<>(
                Arrays.asList(
                        new Person("person1", 45, null),
                        new Person("person2", 39, null),
                        new Person("person3", 60, null),
                        new Person("person4", 45, null),
                        new Person("person5", 43, null)
                )
        );
        IMap<String, Person> map = instance.getMap(mapName);
        map.addIndex(new IndexConfig(IndexType.SORTED, "age").setName(orderedIndexName));

        insertIntoMap(map, people);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = IndexIterationPointer.create(10, true, 100, true, false, null);

        MapOperationProvider operationProvider = getOperationProvider(map);
        MapOperation operation = operationProvider.createFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 10);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        try {
            InvocationFuture<MapFetchIndexOperationResult> future = operationService.createInvocationBuilder(
                    MapService.SERVICE_NAME, operation, address).invoke();
            future.get();
        } catch (Exception e) {
            assertInstanceOf(MissingPartitionException.class, e.getCause());
        } finally {
            instance.shutdown();
        }
    }

    private MapOperationProvider getOperationProvider(IMap<?, ?> map) {
        MapProxyImpl<?, ?> mapProxy = (MapProxyImpl<?, ?>) map;
        MapServiceContext mapServiceContext = mapProxy.getService().getMapServiceContext();
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

    private static void assertResult(MapFetchIndexOperationResult result, List<Person> peopleList) {
        List<QueryableEntry<?, ?>> entries = result.getEntries();
        assertEquals(entries.size(), peopleList.size());

        Map<String, Person> m = new HashMap<>();

        result.getEntries().forEach(e -> m.put((String) e.getKey(), (Person) e.getValue()));
        peopleList.forEach(p -> assertEquals(m.get(p.name), p));
    }

    private static void assertResultSorted(MapFetchIndexOperationResult result, List<Person> orderedPeopleList) {
        List<QueryableEntry<?, ?>> entries = result.getEntries();
        assertEquals(orderedPeopleList.size(), entries.size());

        Iterator<QueryableEntry<?, ?>> entriesIterator = entries.iterator();
        Iterator<Person> expectedIterator = orderedPeopleList.iterator();

        while (entriesIterator.hasNext()) {
            QueryableEntry<?, ?> entry = entriesIterator.next();
            Person expected = expectedIterator.next();

            assertEquals(expected.getName(), entry.getKey());
            assertEquals(expected, entry.getValue());
        }
    }

    static class Person implements Serializable {
        private String name;
        private Integer age;
        private String department;

        Person(String name, Integer age, String department) {
            this.name = name;
            this.age = age;
            this.department = department;
        }

        public String getName() {
            return this.name;
        }

        public Integer getAge() {
            return this.age;
        }

        public String getDepartment() {
            return this.department;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public void setDepartment(String department) {
            this.department = department;
        }

        @Override
        public String toString() {
            return "Person{" + "name='" + name + '\''
                    + ", age=" + age + ", department='"
                    + department + '\'' + '}';
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
            return Objects.equals(name, person.name)
                    && Objects.equals(age, person.age)
                    && Objects.equals(department, person.department);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age, department);
        }
    }

    // Classes needed to mock (modify) MapService

    // Mocking for getMigrationStamp() method.
    // Other overrides are required to prevent exceptions.
    static class MockMapService extends MapService {
        private final MapService delegate;
        private int methodCalled = 0;

        MockMapService(MapService mapService) {
            this.delegate = mapService;
        }

        @Override
        public int getMigrationStamp() {
            methodCalled++;
            if (methodCalled == 2) {
                return delegate.getMigrationStamp() + 1;
            }
            return delegate.getMigrationStamp();
        }

        @Override
        public void init(NodeEngine nodeEngine, Properties properties) {
            delegate.init(nodeEngine, properties);
        }

        @Override
        public void shutdown(boolean terminate) {
            delegate.shutdown(terminate);
        }

        @Override
        public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
            return delegate.createDistributedObject(objectName, source, local);
        }

        @Override
        public MapServiceContext getMapServiceContext() {
            return delegate.getMapServiceContext();
        }
    }

    private static class MockingNodeContext implements NodeContext {
        private final NodeContext delegate;

        MockingNodeContext(NodeContext delegate) {
            super();
            this.delegate = delegate;
        }

        @Override
        public NodeExtension createNodeExtension(Node node) {
            return new CustomMapServiceNodeExtension(node);
        }

        @Override
        public AddressPicker createAddressPicker(Node node) {
            return delegate.createAddressPicker(node);
        }

        @Override
        public Joiner createJoiner(Node node) {
            return delegate.createJoiner(node);
        }

        @Override
        public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
            return delegate.createServer(node, registry, addressRegistry);
        }
    }

    private static class CustomMapServiceNodeExtension extends DefaultNodeExtension {
        CustomMapServiceNodeExtension(Node node) {
            super(node);
        }

        @Override
        public <T> T createService(Class<T> clazz, Object... params) {
            return clazz.isAssignableFrom(MapService.class)
                    ? (T) new MockMapService((MapService) super.createService(clazz, params)) : super.createService(clazz, params);
        }
    }

    private static class CustomTestInstanceFactory extends TestHazelcastInstanceFactory {

        @Override
        public HazelcastInstance newHazelcastInstance(Config config) {
            String instanceName = config != null ? config.getInstanceName() : null;
            NodeContext nodeContext;
            if (TestEnvironment.isMockNetwork()) {
                config = initOrCreateConfig(config);
                nodeContext = this.registry.createNodeContext(this.nextAddress(config.getNetworkConfig().getPort()));
            } else {
                nodeContext = new DefaultNodeContext();
            }
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, new MockingNodeContext(nodeContext));
        }
    }
}
