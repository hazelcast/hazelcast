/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.impl.connector.AbstractIndexReader;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MissingPartitionException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.plan.node.MapIndexScanMetadata;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.getHazelcastInstance;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.comparisonFn;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.jet.sql.impl.connector.map.MapIndexScanUtils.intValue;
import static com.hazelcast.spi.impl.InternalCompletableFuture.completedExceptionally;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.sql.impl.SqlTestSupport.valuePath;
import static com.hazelcast.sql.impl.expression.ColumnExpression.create;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("rawtypes")
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexScanPMigrationHandlingTest extends JetTestSupport {
    static final int itemsCount = 1000;

    @Parameterized.Parameters(name = "instanceCount:{0}")
    public static Collection<Integer> parameters() {
        return asList(2, 3);
    }

    @Parameterized.Parameter(0)
    public int instanceCount;

    private HazelcastInstance[] instances;
    private IMap<Integer, Person> map;

    private final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(instanceCount);

    @Before
    public void before() {
        instances = new HazelcastInstance[instanceCount];
        for (int i = 0; i < instanceCount; ++i) {
            instances[i] = FACTORY.newHazelcastInstance(smallInstanceConfig());
        }
        map = instances[0].getMap(randomMapName());
    }

    @After
    public void after() {
        for (int i = 0; i < instanceCount; ++i) {
            instances[i].shutdown();
        }
    }

    @Test
    public void testConcurrentMigrationHandling() throws Exception {
        for (int i = itemsCount; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age").setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(itemsCount), true);
        List<Expression<?>> projections = asList(create(0, INT), create(1, VARCHAR), create(2, INT));

        MapIndexScanMetadata scanMeta = new MapIndexScanMetadata(
                map.getName(),
                indexConfig.getName(),
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                Arrays.asList(QueryPath.KEY_PATH, valuePath("name"), valuePath("age")),
                Arrays.asList(INT, VARCHAR, INT),
                filter,
                projections,
                projections,
                null,
                comparisonFn(singletonList(new FieldCollation(new RelFieldCollation(2))))
        );

        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instances[0]);
        InternalSerializationService iss = (InternalSerializationService) nodeEngineImpl.getSerializationService();
        ExpressionEvalContext evalContext = new SimpleExpressionEvalContext(emptyList(), iss);

        List<Address> addresses = new ArrayList<>();
        List<int[]> currentPartitions = new ArrayList<>();
        int[] migratedPartitions = new int[instanceCount - 1];
        Map<Address, int[]> assignments = ExecutionPlanBuilder.getPartitionAssignment(nodeEngineImpl);

        for (int i = 0; i < instanceCount; ++i) {
            Address currentAddress = instances[i].getCluster().getLocalMember().getAddress();
            addresses.add(currentAddress);
            currentPartitions.add(assignments.get(currentAddress));
        }

        MapIndexScanP processor = new MapIndexScanP<>(new MockReader(), instances[0], evalContext, currentPartitions.get(0), scanMeta);

        // Reshuffle partitions : 'migrate' last partitions from Member1 to Member(`instanceCount - 1`)
        Map<Address, int[]> newAssignedPartitions = new HashMap<>();

        int newMigratedPartitionsCapacity = currentPartitions.get(0).length - instanceCount + 1;
        int[] newDrainedPartitionsSet = new int[newMigratedPartitionsCapacity];
        System.arraycopy(currentPartitions.get(0), 0, newDrainedPartitionsSet, 0, newMigratedPartitionsCapacity);
        newAssignedPartitions.put(addresses.get(0), newDrainedPartitionsSet);

        for (int i = 1; i < instanceCount; ++i) {
            int[] newPartitionsSet = new int[currentPartitions.get(i).length + 1];
            System.arraycopy(currentPartitions.get(i), 0, newPartitionsSet, 0, currentPartitions.get(i).length);
            int migratedPartition = currentPartitions.get(0)[currentPartitions.get(0).length - i];
            newPartitionsSet[currentPartitions.get(i).length] = migratedPartition;
            migratedPartitions[i - 1] = migratedPartition;
            newAssignedPartitions.put(addresses.get(i), newPartitionsSet);
        }

        PartitionIdSet ownedPartitionSetByFirstMember = new PartitionIdSet(11, newDrainedPartitionsSet);

        processor.init(
                new TestOutbox(1),
                new TestProcessorContext()
                        .setHazelcastInstance(instances[0])
                        .setPartitionAssignment(newAssignedPartitions)
                        .setJobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
        );
        List<MapIndexScanP.Split> split = processor.splitOnMigration(0);
        assertEquals(instanceCount, split.size());
        // 'Drained' member is always located on the last index because `splitOnMigration()` puts owned partitions last.
        // Also it's a reason of such strange `instanceCount - i - 2` shift in assertion below.
        for (int i = 0; i < instanceCount - 1; ++i) {
            assertTrue(split.get(instanceCount - i - 2).getPartitions().contains(migratedPartitions[i]));
        }
        assertTrue(split.get(split.size() - 1).getPartitions().containsAll(ownedPartitionSetByFirstMember));
        assertFalse(processor.complete());
    }

    static class MockReader extends AbstractIndexReader<
            InternalCompletableFuture<MapFetchIndexOperationResult>,
            MapFetchIndexOperationResult,
            QueryableEntry<?, ?>> {

        private HazelcastInstance hazelcastInstance;
        private boolean isDone = false;

        @Nonnull
        @Override
        public InternalCompletableFuture<MapFetchIndexOperationResult> readBatch(
                Address address,
                PartitionIdSet partitions,
                IndexIterationPointer[] pointers
        ) {
            if (!isDone) {
                isDone = true;
                return newCompletedFuture(1);
            }
            return completedExceptionally(new MissingPartitionException("Mock"));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(hazelcastInstance.getName());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            hazelcastInstance = getHazelcastInstance(in.readString());
        }
    }

    static class Person implements DataSerializable {
        private String name;
        private int age;

        Person() {
            // no op.
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(name);
            out.writeInt(age);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readString();
            this.age = in.readInt();
        }
    }
}

