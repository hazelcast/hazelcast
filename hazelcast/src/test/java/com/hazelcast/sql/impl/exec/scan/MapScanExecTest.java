/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.scan;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("unused")
public class MapScanExecTest extends SqlTestSupport {

    private static final int BATCH_SIZE = MapScanExec.BATCH_SIZE;
    private static final int PARTITION_COUNT = 10;

    private static final String MAP_OBJECT = "mo";
    private static final String MAP_BINARY = "mb";

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(2);

    private static HazelcastInstance instance1;
    private static HazelcastInstance instance2;

    @BeforeClass
    public static void beforeClass() {
        instance1 = FACTORY.newHazelcastInstance(getInstanceConfig());
        instance2 = FACTORY.newHazelcastInstance(getInstanceConfig());
    }

    @AfterClass
    public static void afterClass() {
        FACTORY.shutdownAll();
    }

    @Before
    public void before() {
        instance1.getMap(MAP_OBJECT).clear();
        instance2.getMap(MAP_BINARY).clear();
    }

    @Test
    public void testNormal_Object() {
        checkNormal(instance1.getMap(MAP_OBJECT));
    }

    @Test
    public void testNormal_Binary() {
        checkNormal(instance1.getMap(MAP_BINARY));
    }

    private void checkNormal(IMap<TestKey, TestValue> map) {
        // Clear previous data.
        map.clear();

        // Get local partitions.
        MapProxyImpl<TestKey, TestValue> mapProxy = ((MapProxyImpl<TestKey, TestValue>) map);
        HazelcastInstance instance = mapProxy.getNodeEngine().getHazelcastInstance();

        Set<Integer> parts = new HashSet<>();
        PartitionIdSet parts0 = new PartitionIdSet(PARTITION_COUNT);

        for (Partition partition : instance.getPartitionService().getPartitions()) {
            if (instance.getLocalEndpoint().getUuid().equals(partition.getOwner().getUuid())) {
                parts.add(partition.getPartitionId());
                parts0.add(partition.getPartitionId());
            }
        }

        // Test scan on empty map.
        checkScanResults(mapProxy, parts0, null, new TreeSet<>());

        // Load data that belongs to these partitions.
        TreeSet<Integer> allResults = new TreeSet<>();
        TreeSet<Integer> filterResults = new TreeSet<>();
        boolean shouldPass = true;

        int loaded = 0;
        int currentKey = 0;

        while (loaded < BATCH_SIZE * 3 / 2) {
            TestKey key = new TestKey(currentKey);
            int keyPartition = instance.getPartitionService().getPartition(key).getPartitionId();

            if (parts.contains(keyPartition)) {
                map.put(key, new TestValue(currentKey, shouldPass));

                allResults.add(currentKey);

                if (shouldPass) {
                    filterResults.add(currentKey);

                    loaded++;
                }

                shouldPass = !shouldPass;
            }

            currentKey++;
        }

        // Run without filter.
        checkScanResults(mapProxy, parts0, null, allResults);

        // Run with filter.
        checkScanResults(mapProxy, parts0, new TestFilter(2), filterResults);
    }

    private void checkScanResults(
        MapProxyImpl<TestKey, TestValue> mapProxy,
        PartitionIdSet parts,
        Expression<Boolean> filter,
        TreeSet<Integer> expectedResults
    ) {
        int id = 1;
        MapContainer mapContainer = mapProxy.getService().getMapServiceContext().getMapContainer(mapProxy.getName());
        List<QueryPath> fieldPaths = Arrays.asList(keyPath("val1"), valuePath("val2"), valuePath("val3"));
        List<QueryDataType> fieldTypes = Arrays.asList(QueryDataType.INT, QueryDataType.BIGINT, QueryDataType.BOOLEAN);
        List<Integer> projects = Arrays.asList(0, 1);
        InternalSerializationService serializationService =
            (InternalSerializationService) mapProxy.getNodeEngine().getSerializationService();

        MapScanExec exec = new MapScanExec(
            id,
            mapContainer,
            parts,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            fieldPaths,
            fieldTypes,
            projects,
            filter,
            serializationService
        );

        assertEquals(id, exec.getId());
        assertEquals(mapContainer, exec.getMap());
        assertEquals(parts, exec.getPartitions());
        assertEquals(GenericQueryTargetDescriptor.INSTANCE, exec.getKeyDescriptor());
        assertEquals(GenericQueryTargetDescriptor.INSTANCE, exec.getValueDescriptor());
        assertEquals(fieldPaths, exec.getFieldPaths());
        assertEquals(fieldTypes, exec.getFieldTypes());
        assertEquals(projects, exec.getProjects());
        assertEquals(filter, exec.getFilter());

        exec.setup(emptyFragmentContext());

        TreeSet<Integer> results = new TreeSet<>();

        while (true) {
            IterationResult res = exec.advance();

            RowBatch batch = exec.currentBatch();

            for (int i = 0; i < batch.getRowCount(); i++) {
                int val1 = batch.getRow(i).get(0);
                long val2 = batch.getRow(i).get(1);

                assertEquals(val1, (int) val2);

                results.add(val1);
            }

            if (res == IterationResult.FETCHED_DONE) {
                break;
            }
        }

        assertEquals(expectedResults, results);
    }

    @SuppressWarnings("unchecked")
    private static BiTuple<Integer, Integer> getLocalKey(IMap<TestKey, ?> map) {
        MapProxyImpl<TestKey, ?> mapProxy = ((MapProxyImpl<TestKey, ?>) map);
        PartitionService partitionService = mapProxy.getNodeEngine().getHazelcastInstance().getPartitionService();

        int key = 0;

        while (key < 1000) {
            TestKey key0 = new TestKey(key);

            Partition partition = partitionService.getPartition(key0);

            if (partition.getOwner().localMember()) {
                return BiTuple.of(key, partition.getPartitionId());
            }

            key++;
        }

        throw new RuntimeException("Failed to get local key!");
    }

    @Test
    public void testSerializationError() {
        IMap<TestKey, TestBadValue> map = instance1.getMap(MAP_BINARY);
        MapProxyImpl<TestKey, TestBadValue> mapProxy = ((MapProxyImpl<TestKey, TestBadValue>) map);

        BiTuple<Integer, Integer> localKeyTuple = getLocalKey(map);

        map.put(new TestKey(localKeyTuple.element1()), new TestBadValue(1L, true));

        PartitionIdSet partitionIdSet = new PartitionIdSet(PARTITION_COUNT);
        partitionIdSet.add(localKeyTuple.element2());

        MapScanExec exec = new MapScanExec(
            1,
            mapProxy.getService().getMapServiceContext().getMapContainer(mapProxy.getName()),
            partitionIdSet,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            Collections.singletonList(valuePath("val2")),
            Collections.singletonList(QueryDataType.BIGINT),
            Collections.singletonList(0),
            null,
            (InternalSerializationService) mapProxy.getNodeEngine().getSerializationService()
        );

        exec.setup(emptyFragmentContext());

        QueryException exception = assertThrows(QueryException.class, exec::advance);
        assertEquals(SqlErrorCode.DATA_EXCEPTION, exception.getCode());
        assertEquals(HazelcastSerializationException.class, exception.getCause().getClass());
    }

    @Test
    public void testIncorrectType() {
        IMap<TestKey, TestValue> map = instance1.getMap(MAP_BINARY);
        MapProxyImpl<TestKey, TestValue> mapProxy = ((MapProxyImpl<TestKey, TestValue>) map);

        BiTuple<Integer, Integer> localKeyTuple = getLocalKey(map);

        map.put(new TestKey(localKeyTuple.element1()), new TestValue(1L, true));

        PartitionIdSet partitionIdSet = new PartitionIdSet(PARTITION_COUNT);
        partitionIdSet.add(localKeyTuple.element2());

        MapScanExec exec = new MapScanExec(
            1,
            mapProxy.getService().getMapServiceContext().getMapContainer(mapProxy.getName()),
            partitionIdSet,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            Collections.singletonList(valuePath("val2")),
            Collections.singletonList(QueryDataType.TIMESTAMP),
            Collections.singletonList(0),
            null,
            (InternalSerializationService) mapProxy.getNodeEngine().getSerializationService()
        );

        exec.setup(emptyFragmentContext());

        QueryException exception = assertThrows(QueryException.class, exec::advance);
        assertEquals(SqlErrorCode.DATA_EXCEPTION, exception.getCode());

        QueryException cause = (QueryException) exception.getCause();
        assertEquals(SqlErrorCode.DATA_EXCEPTION, cause.getCode());
        assertTrue(cause.getMessage().contains("Cannot convert BIGINT to TIMESTAMP"));
    }

    /**
     * Simulates the case when the partition is migrated out of the member *BEFORE* the query is started, and therefore
     * migration stamps cannot help.
     */
    @Test
    public void testMissingPartition() {
        IMap<TestKey, TestValue> localMap = instance1.getMap(MAP_BINARY);
        MapProxyImpl<TestKey, TestValue> localMapProxy = ((MapProxyImpl<TestKey, TestValue>) localMap);

        IMap<TestKey, TestValue> remoteMap = instance2.getMap(MAP_BINARY);
        int remotePartition = getLocalKey(remoteMap).element2();

        PartitionIdSet partitionIdSet = new PartitionIdSet(PARTITION_COUNT);
        partitionIdSet.add(remotePartition);

        MapScanExec exec = new MapScanExec(
            1,
            localMapProxy.getService().getMapServiceContext().getMapContainer(localMapProxy.getName()),
            partitionIdSet,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            Collections.singletonList(valuePath("val2")),
            Collections.singletonList(QueryDataType.TIMESTAMP),
            Collections.singletonList(0),
            null,
            (InternalSerializationService) localMapProxy.getNodeEngine().getSerializationService()
        );

        QueryException exception = assertThrows(QueryException.class, () -> exec.setup(emptyFragmentContext()));
        assertEquals(SqlErrorCode.PARTITION_MIGRATED, exception.getCode());
    }

    /**
     * Simulates the case when partitions are migrated during query execution. Tp achieve this we load keys into local
     * paritions in a way that iteration stops before the first partition is read. Then we start the new member, that
     * chagnes the migration stamp. Then we try to read the remaining data.
     */
    @Test
    public void testConcurrentMigration() {
        Map<TestKey, TestValue> map = instance1.getMap(MAP_OBJECT);
        MapProxyImpl<TestKey, TestValue> mapProxy = ((MapProxyImpl<TestKey, TestValue>) map);
        PartitionService partitionService = instance1.getPartitionService();

        // Get local partition.
        int localPartition = -1;

        for (int i = 0; i < PARTITION_COUNT; i++) {
            for (Partition partition : instance1.getPartitionService().getPartitions()) {
                if (instance1.getLocalEndpoint().getUuid().equals(partition.getOwner().getUuid())) {
                    localPartition = partition.getPartitionId();

                    break;
                }
            }
        }

        assertNotEquals(-1, localPartition);

        // Load entries into the partition such that is spans several batches.
        int loaded = 0;
        int currentKey = 0;

        while (loaded < BATCH_SIZE * 3 / 2) {
            TestKey key = new TestKey(currentKey);
            int partition = partitionService.getPartition(key).getPartitionId();

            if (partition == localPartition) {
                map.put(key, new TestValue(1L, true));

                loaded++;
            }

            currentKey++;
        }

        // Start execution.
        PartitionIdSet partitionIdSet = new PartitionIdSet(PARTITION_COUNT);
        partitionIdSet.add(localPartition);

        MapScanExec exec = new MapScanExec(
            1,
            mapProxy.getService().getMapServiceContext().getMapContainer(mapProxy.getName()),
            partitionIdSet,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            Collections.singletonList(valuePath("val2")),
            Collections.singletonList(QueryDataType.BIGINT),
            Collections.singletonList(0),
            null,
            (InternalSerializationService) mapProxy.getNodeEngine().getSerializationService()
        );

        exec.setup(emptyFragmentContext());

        assertEquals(IterationResult.FETCHED, exec.advance());

        // Add member
        int migrationStamp = mapProxy.getService().getMigrationStamp();

        HazelcastInstance instance3 = FACTORY.newHazelcastInstance(getInstanceConfig());

        try {
            // Await for migration stamp to change.
            assertTrueEventually(() -> assertNotEquals(migrationStamp, mapProxy.getService().getMigrationStamp()));

            // Try advance, should fail.
            QueryException exception = assertThrows(QueryException.class, exec::advance);
            assertEquals(SqlErrorCode.PARTITION_MIGRATED, exception.getCode());
        } finally {
            instance3.shutdown();
        }
    }

    @Test
    public void testConcurrentMapDestroy() {
        // Collect local partitions.
        PartitionIdSet partitionIdSet = new PartitionIdSet(PARTITION_COUNT);

        for (int i = 0; i < PARTITION_COUNT; i++) {
            for (Partition partition : instance1.getPartitionService().getPartitions()) {
                if (instance1.getLocalEndpoint().getUuid().equals(partition.getOwner().getUuid())) {
                    partitionIdSet.add(partition.getPartitionId());

                    break;
                }
            }
        }

        // Put one key to each partition.
        IMap<TestKey, TestValue> map = instance1.getMap(randomMapName());
        MapProxyImpl<TestKey, TestValue> mapProxy = (MapProxyImpl<TestKey, TestValue>) map;

        for (int partition : partitionIdSet) {
            int currentKey = 0;

            while (true) {
                TestKey key = new TestKey(currentKey);

                if (instance1.getPartitionService().getPartition(key).getPartitionId() == partition) {
                    map.put(key, new TestValue(1L, true));

                    break;
                }

                currentKey++;
            }
        }

        // Prepare executor.
        MapScanExec exec = new MapScanExec(
            1,
            mapProxy.getService().getMapServiceContext().getMapContainer(mapProxy.getName()),
            partitionIdSet,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            Collections.singletonList(valuePath("val2")),
            Collections.singletonList(QueryDataType.BIGINT),
            Collections.singletonList(0),
            null,
            (InternalSerializationService) mapProxy.getNodeEngine().getSerializationService()
        );

        exec.setup(emptyFragmentContext());

        // Destroy the map.
        map.destroy();

        // Advance after destroy.
        QueryException exception = assertThrows(QueryException.class, exec::advance);
        assertEquals(SqlErrorCode.MAP_DESTROYED, exception.getCode());
    }

    @Test
    public void testExpiration() throws Exception {
        IMap<TestKey, TestValue> map = instance1.getMap(MAP_BINARY);
        MapProxyImpl<TestKey, TestValue> mapProxy = ((MapProxyImpl<TestKey, TestValue>) map);

        BiTuple<Integer, Integer> localKeyTuple = getLocalKey(map);

        map.put(new TestKey(localKeyTuple.element1()), new TestValue(1L, true), 1, TimeUnit.MILLISECONDS);

        Thread.sleep(1500);

        PartitionIdSet partitionIdSet = new PartitionIdSet(PARTITION_COUNT);
        partitionIdSet.add(localKeyTuple.element2());

        MapScanExec exec = new MapScanExec(
            1,
            mapProxy.getService().getMapServiceContext().getMapContainer(mapProxy.getName()),
            partitionIdSet,
            GenericQueryTargetDescriptor.INSTANCE,
            GenericQueryTargetDescriptor.INSTANCE,
            Collections.singletonList(valuePath("val2")),
            Collections.singletonList(QueryDataType.BIGINT),
            Collections.singletonList(0),
            null,
            (InternalSerializationService) mapProxy.getNodeEngine().getSerializationService()
        );

        exec.setup(emptyFragmentContext());

        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        assertEquals(0, exec.currentBatch().getRowCount());
    }

    private static Config getInstanceConfig() {
        return new Config()
            .addMapConfig(new MapConfig().setName(MAP_OBJECT).setInMemoryFormat(InMemoryFormat.OBJECT))
            .addMapConfig(new MapConfig().setName(MAP_BINARY).setInMemoryFormat(InMemoryFormat.BINARY))
            .setProperty("hazelcast.partition.count", Integer.toString(PARTITION_COUNT));
    }

    private static class TestKey implements DataSerializable {

        private int val1;

        private TestKey() {
            // No-op.
        }

        private TestKey(int val1) {
            this.val1 = val1;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(val1);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            val1 = in.readInt();
        }
    }

    private static class TestValue implements DataSerializable {

        private long val2;
        private boolean val3;

        private TestValue() {
            // No-op.
        }

        private TestValue(long val2, boolean val3) {
            this.val2 = val2;
            this.val3 = val3;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(val2);
            out.writeBoolean(val3);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            val2 = in.readLong();
            val3 = in.readBoolean();
        }
    }

    private static class TestBadValue extends TestValue {
        private TestBadValue() {
            // No-op.
        }

        private TestBadValue(long val2, boolean val3) {
            super(val2, val3);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new IOException("Error!");
        }
    }

    private static class TestFilter implements Expression<Boolean>, DataSerializable {

        private int index;

        private TestFilter() {
            // No-op.
        }

        private TestFilter(int index) {
            this.index = index;
        }

        @Override
        public Boolean eval(Row row, ExpressionEvalContext context) {
            return (Boolean) row.get(index);
        }

        @Override
        public QueryDataType getType() {
            return QueryDataType.BOOLEAN;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(index);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            index = in.readInt();
        }
    }
}
