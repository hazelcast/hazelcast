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

package com.hazelcast.map;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getOperationService;
import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexLifecycleTest extends HazelcastTestSupport {

    private static final int BOOK_COUNT = 1000;

    private String mapName = randomMapName();

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    boolean globalIndex() {
        return true;
    }

    @Test
    public void recordStoresAndIndexes_createdDestroyedProperly() {
        // GIVEN
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = createNode(instanceFactory);

        // THEN - initialized
        IMap bookMap = instance1.getMap(mapName);
        fillMap(bookMap);
        assertEquals(BOOK_COUNT, bookMap.size());
        assertAllPartitionContainersAreInitialized(instance1);

        // THEN - destroyed
        bookMap.destroy();
        assertAllPartitionContainersAreEmpty(instance1);

        // THEN - initialized
        bookMap = instance1.getMap(mapName);
        fillMap(bookMap);
        assertEquals(BOOK_COUNT, bookMap.size());
        assertAllPartitionContainersAreInitialized(instance1);
    }

    @Test
    public void whenIndexConfigured_existsOnAllMembers() {
        // GIVEN indexes are configured before Hazelcast starts
        int clusterSize = 3;
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];

        instances[0] = createNode(instanceFactory);
        IMap<Integer, Book> bookMap = instances[0].getMap(mapName);
        fillMap(bookMap);
        assertEquals(BOOK_COUNT, bookMap.size());

        // THEN indexes are migrated and populated on all members
        for (int i = 1; i < clusterSize; i++) {
            instances[i] = createNode(instanceFactory);
            waitAllForSafeState(copyOfRange(instances, 0, i + 1));
            bookMap = instances[i].getMap(mapName);
            assertEquals(BOOK_COUNT, bookMap.keySet().size());
            assertAllPartitionContainersAreInitialized(instances[i]);
            assertGlobalIndexesAreInitialized(instances[i]);
        }
    }

    @Test
    public void whenIndexAddedProgrammatically_existsOnAllMembers() {
        // GIVEN indexes are configured before Hazelcast starts
        int clusterSize = 3;
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(clusterSize);
        HazelcastInstance[] instances = new HazelcastInstance[clusterSize];

        Config config = getConfig().setProperty(ClusterProperty.PARTITION_COUNT.getName(), "4");
        config.getMapConfig(mapName);
        ConfigAccessor.getServicesConfig(config)
                .addServiceConfig(
                        new ServiceConfig()
                                .setName("SlowPostJoinAwareService")
                                .setEnabled(true)
                                .setImplementation(new SlowPostJoinAwareService())
                );

        instances[0] = instanceFactory.newHazelcastInstance(config);
        IMap<Integer, Book> bookMap = instances[0].getMap(mapName);
        fillMap(bookMap);
        bookMap.addIndex(getIndexConfig("author", false));
        bookMap.addIndex(getIndexConfig("year", true));
        assertEquals(BOOK_COUNT, bookMap.size());

        // THEN indexes are migrated and populated on all members
        for (int i = 1; i < clusterSize; i++) {
            instances[i] = instanceFactory.newHazelcastInstance(config);
            waitAllForSafeState(copyOfRange(instances, 0, i + 1));
            bookMap = instances[i].getMap(mapName);
            assertEquals(BOOK_COUNT, bookMap.keySet().size());
            assertAllPartitionContainersAreInitialized(instances[i]);
            assertGlobalIndexesAreInitialized(instances[i]);
        }
    }

    private void assertGlobalIndexesAreInitialized(HazelcastInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        final MapContainer mapContainer = context.getMapContainer(mapName);
        if (!globalIndex()) {
            return;
        }
        assertTrueEventually(() -> assertEquals(2, mapContainer.getIndexes().getIndexes().length));

        assertNotNull("There should be a global index for attribute 'author'",
                mapContainer.getIndexes().getIndex("author"));
        assertNotNull("There should be a global index for attribute 'year'",
                mapContainer.getIndexes().getIndex("year"));
        final String authorOwned = findAuthorOwnedBy(instance);
        final Integer yearOwned = findYearOwnedBy(instance);
        assertTrueEventually(() -> assertTrue("Author index should contain records.",
                mapContainer.getIndexes()
                        .getIndex("author")
                        .getRecords(authorOwned).size() > 0));

        assertTrueEventually(() -> assertTrue("Year index should contain records",
                mapContainer.getIndexes().getIndex("year").getRecords(yearOwned).size() > 0));
    }

    private int numberOfPartitionQueryResults(HazelcastInstance instance, int partitionId, String attribute, Comparable value) {
        OperationService operationService = getOperationService(instance);
        Query query = Query.of().mapName(mapName).iterationType(IterationType.KEY).predicate(Predicates.equal(attribute, value))
                .build();
        Operation queryOperation = getMapOperationProvider(instance, mapName).createQueryPartitionOperation(query);
        InternalCompletableFuture<QueryResult> future = operationService
                .invokeOnPartition(MapService.SERVICE_NAME, queryOperation, partitionId);
        return future.join().size();
    }

    private void assertAllPartitionContainersAreEmpty(HazelcastInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        int partitionCount = getPartitionCount(instance);

        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer container = context.getPartitionContainer(i);

            Map<String, ?> maps = container.getMaps().entrySet().stream()
                    .filter(e -> !e.getKey().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            assertTrue("record stores not empty", maps.isEmpty());

            Map<String, Indexes> indexes = container.getIndexes()
                    .entrySet().stream()
                    .filter(e -> !e.getKey().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            assertTrue("indexes not empty", indexes.isEmpty());
        }
    }

    private void assertAllPartitionContainersAreInitialized(HazelcastInstance instance) {
        MapServiceContext context = getMapServiceContext(instance);
        int partitionCount = getPartitionCount(instance);
        final AtomicInteger authorRecordsCounter = new AtomicInteger();
        final AtomicInteger yearRecordsCounter = new AtomicInteger();

        String authorOwned = findAuthorOwnedBy(instance);
        Integer yearOwned = findYearOwnedBy(instance);

        for (int i = 0; i < partitionCount; i++) {
            if (!getNode(instance).getPartitionService().isPartitionOwner(i)) {
                continue;
            }

            PartitionContainer container = context.getPartitionContainer(i);

            ConcurrentMap<String, RecordStore> maps = container.getMaps();
            RecordStore recordStore = maps.get(mapName);
            assertNotNull("record store is null: ", recordStore);

            if (!globalIndex()) {
                // also assert contents of partition indexes when NATIVE memory format
                ConcurrentMap<String, Indexes> indexes = container.getIndexes();
                final Indexes index = indexes.get(mapName);
                assertNotNull("indexes is null", indexes);
                assertEquals(2, index.getIndexes().length);
                assertNotNull("There should be a partition index for attribute 'author'", index.getIndex("author"));
                assertNotNull("There should be a partition index for attribute 'year'", index.getIndex("year"));

                authorRecordsCounter.getAndAdd(numberOfPartitionQueryResults(instance, i, "author", authorOwned));
                yearRecordsCounter.getAndAdd(numberOfPartitionQueryResults(instance, i, "year", yearOwned));
            }
        }

        if (!globalIndex()) {
            assertTrue("Author index should contain records", authorRecordsCounter.get() > 0);
            assertTrue("Year index should contain records", yearRecordsCounter.get() > 0);
        }
    }

    private int getPartitionCount(HazelcastInstance instance) {
        Node node = getNode(instance);
        return node.getProperties().getInteger(ClusterProperty.PARTITION_COUNT);
    }

    private MapServiceContext getMapServiceContext(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine1 = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine1.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }


    private HazelcastInstance createNode(TestHazelcastInstanceFactory instanceFactory) {
        Config config = getConfig().setProperty(ClusterProperty.PARTITION_COUNT.getName(), "4");
        config.getMapConfig(mapName)
                .addIndexConfig(getIndexConfig("author", false))
                .addIndexConfig(getIndexConfig("year", true))
                .setBackupCount(1);
        return instanceFactory.newHazelcastInstance(config);
    }

    public static class Book implements Serializable {

        private long id;
        private String title;
        private String author;
        private int year;

        private Book() {
        }

        Book(long id, String title, String author, int year) {
            this.id = id;
            this.title = title;
            this.author = author;
            this.year = year;
        }

        public long getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public String getAuthor() {
            return author;
        }

        public int getYear() {
            return year;
        }
    }

    private void fillMap(IMap<Integer, Book> map) {
        for (int key = 0; key < BOOK_COUNT; key++) {
            map.put(key, new Book(key, String.valueOf(key), getAuthorNameByKey(key), getYearByKey(key)));
        }
    }

    // A CoreService with a slow post-join op. Its post-join operation will be executed before map's
    // post-join operation so we can ensure indexes are created via MapReplicationOperation,
    // even though PostJoinMapOperation has not yet been executed.
    public static class SlowPostJoinAwareService implements CoreService, PostJoinAwareService {
        @Override
        public Operation getPostJoinOperation() {
            return new SlowOperation();
        }
    }

    public static class SlowOperation extends Operation {
        @Override
        public void run() {
            sleepSeconds(60);
        }
    }

    private String findAuthorOwnedBy(HazelcastInstance hz) {
        int ownedKey = 0;
        Member localMember = hz.getCluster().getLocalMember();
        for (int i = 0; i < BOOK_COUNT; i++) {
            if (localMember.equals(hz.getPartitionService().getPartition(i).getOwner())) {
                ownedKey = i;
                break;
            }
        }
        return getAuthorNameByKey(ownedKey);
    }

    private Integer findYearOwnedBy(HazelcastInstance hz) {
        int ownedKey = 0;
        Member localMember = hz.getCluster().getLocalMember();
        for (int i = 0; i < BOOK_COUNT; i++) {
            if (localMember.equals(hz.getPartitionService().getPartition(i).getOwner())) {
                ownedKey = i;
                break;
            }
        }
        return getYearByKey(ownedKey);
    }

    private static int getYearByKey(int key) {
        return 1800 + key % 200;
    }

    private static String getAuthorNameByKey(int key) {
        return String.valueOf(key % 7);
    }

    private static IndexConfig getIndexConfig(String attribute, boolean ordered) {
        return new IndexConfig(ordered ? IndexType.SORTED : IndexType.HASH, attribute).setName(attribute);
    }
}
