/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MissingPartitionException;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.impl.CompositeValue;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
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
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapFetchIndexOperationTest extends HazelcastTestSupport {
    private static final String mapName = "map1";
    private static final String orderedIndexName = "index_age_sorted";
    private static final String compositeOrderedIndexName = "index_age_dep_sorted";
    private static final String hashIndexName = "index_age_hash";

    private HazelcastInstance instance;
    private Config config;

    private IMap<String, Person> map;

    @Parameterized.Parameter
    public boolean descending;

    @Parameterized.Parameters(name = "descending:{0}")
    public static Object[] parameters() {
        return new Object[] { false, true };
    }

    @Before
    public void setup() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);

        config = getConfig();
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

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @After
    public void destroy() {
        instance.shutdown();
    }

    @Test
    public void testRange() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = IndexIterationPointer.create(30, true, 60, false, descending, null);

        MapOperation operation = new MapFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 7);

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
        pointers[0] = IndexIterationPointer.create(null, true, 60, false, !descending, null);

        MapOperation operation = new MapFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 10);

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
                descending,
                null
        );

        MapOperation operation = new MapFetchIndexOperation(mapName, compositeOrderedIndexName, pointers, partitions, 10);

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
        pointers[0] = IndexIterationPointer.create(null, true, null, true, descending, null);

        MapOperation operation = new MapFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 20);

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
    public void testFullScanCompositeIndex() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[1];
        pointers[0] = IndexIterationPointer.create(null, true, null, true, descending, null);

        MapOperation operation = new MapFetchIndexOperation(mapName, compositeOrderedIndexName, pointers, partitions, 20);

        Address address = instance.getCluster().getLocalMember().getAddress();
        OperationServiceImpl operationService = getOperationService(instance);

        MapFetchIndexOperationResult result = operationService.createInvocationBuilder(
                MapService.SERVICE_NAME, operation, address).<MapFetchIndexOperationResult>invoke().get();

        assertResultSorted(result, Arrays.asList(
                new Person("person7", null, null),
                new Person("person6", null, "Dep1"),
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
    public void testMultipleLookupsHashIndex() throws ExecutionException, InterruptedException {
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[3];
        pointers[0] = IndexIterationPointer.create(30, true, 30, true, descending, null);
        pointers[1] = IndexIterationPointer.create(39, true, 39, true, descending, null);
        pointers[2] = IndexIterationPointer.create(45, true, 45, true, descending, null);
        if (descending) {
            Collections.reverse(Arrays.asList(pointers));
        }

        MapOperation operation = new MapFetchIndexOperation(mapName, hashIndexName, pointers, partitions, 10);

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
        pointers[0] = IndexIterationPointer.create(30, true, 40, true, descending, null);
        pointers[1] = IndexIterationPointer.create(50, true, 60, true, descending, null);
        if (descending) {
            Collections.reverse(Arrays.asList(pointers));
        }

        MapOperation operation = new MapFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 5);

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
        assumeFalse(descending);
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[2];
        pointers[0] = IndexIterationPointer.create(30, true, 50, false, false, null);
        pointers[1] = IndexIterationPointer.create(50, true, 60, true, false, null);

        MapOperation operation = new MapFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 5);

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

        operation = new MapFetchIndexOperation(
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
        assumeFalse(descending);
        PartitionIdSet partitions = getLocalPartitions(instance);

        IndexIterationPointer[] pointers = new IndexIterationPointer[2];
        pointers[0] = IndexIterationPointer.create(50, true, 60, true, true, null);
        pointers[1] = IndexIterationPointer.create(30, true, 50, false, true, null);

        MapOperation operation = new MapFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 5);

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

        operation = new MapFetchIndexOperation(
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
        pointers[0] = IndexIterationPointer.create(10, true, 100, true, descending, null);

        MapOperation operation = new MapFetchIndexOperation(mapName, orderedIndexName, pointers, partitions, 10);

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

    @Test
    public void test_MapFetchIndexOperation_serialization() {
        MapFetchIndexOperation op = new MapFetchIndexOperation("m", "i", new IndexIterationPointer[0], new PartitionIdSet(271), 100);
        SerializationService ss = getNodeEngineImpl(instance).getSerializationService();
        MapFetchIndexOperation cloned = ss.toObject(ss.toData(op));
        assertThat(op).usingRecursiveComparison().isEqualTo(cloned);
    }

    @Test
    public void test_MapFetchIndexOperationResult_serialization() {
        MapFetchIndexOperationResult op = new MapFetchIndexOperationResult(emptyList(), new IndexIterationPointer[0]);
        SerializationService ss = getNodeEngineImpl(instance).getSerializationService();
        MapFetchIndexOperationResult cloned = ss.toObject(ss.toData(op));
        assertThat(op).usingRecursiveComparison().isEqualTo(cloned);
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

    private void assertResultSorted(MapFetchIndexOperationResult result, List<Person> orderedPeopleList) {
        List<QueryableEntry<?, ?>> entries = result.getEntries();
        List<Tuple2> resultsList = entries.stream().map(e -> Tuple2.tuple2(e.getKey(), e.getValue())).collect(Collectors.toList());
        List<Tuple2> people = orderedPeopleList.stream().map(e -> Tuple2.tuple2(e.getName(), e)).collect(Collectors.toList());
        if (descending) {
            Collections.reverse(people);
        }
        assertThat(resultsList).containsExactlyElementsOf(people);
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
