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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.connector.AbstractIndexReader;
import com.hazelcast.jet.impl.execution.init.ExecutionPlanBuilder;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MissingPartitionException;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.plan.node.MapIndexScanMetadata;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.getHazelcastInstance;
import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.comparisonFn;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.spi.impl.InternalCompletableFuture.completedExceptionally;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.sql.impl.SqlTestSupport.valuePath;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("rawtypes")
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexScanPMigrationHandlingTest extends SimpleTestInClusterSupport {
    static final int itemsCount = 100_000;
    private IMap<Integer, Person> map;
    HazelcastInstance instance1;
    HazelcastInstance instance2;

    private final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(2);

    @Before
    public void before() {
        instance1 = FACTORY.newHazelcastInstance(smallInstanceConfig());
        instance2 = FACTORY.newHazelcastInstance(smallInstanceConfig());
        map = instance1.getMap(randomName());
    }

    @After
    public void after() {
        instance1.shutdown();
        instance2.shutdown();
    }

    @Test
    public void testConcurrentMigrationHandlingWithMock() throws Exception {
        List<Object[]> expected = new ArrayList<>();
        for (int i = itemsCount; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            expected.add(new Object[]{(itemsCount - i + 1), "value-" + (itemsCount - i + 1), (itemsCount - i + 1)});
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age");
        indexConfig.setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(itemsCount), true);
        List<Expression<?>> projections = asList(
                ColumnExpression.create(0, INT),
                ColumnExpression.create(1, VARCHAR),
                ColumnExpression.create(2, INT)
        );

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

        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance1);
        InternalSerializationService serializationService =
                (InternalSerializationService) nodeEngineImpl.getSerializationService();
        ExpressionEvalContext evalContext = new SimpleExpressionEvalContext(emptyList(), serializationService);
        Address address1 = instance1.getCluster().getLocalMember().getAddress();
        Address address2 = instance2.getCluster().getLocalMember().getAddress();
        Map<Address, int[]> assignments = ExecutionPlanBuilder.getPartitionAssignment(nodeEngineImpl);
        int[] partitions1 = assignments.get(address1);
        int[] partitions2 = assignments.get(address2);

        MapIndexScanP processor = new MapIndexScanP<>(new MockReader(), instance1, evalContext, partitions1, scanMeta);

        int[] newPartitionsSet1 = new int[partitions1.length - 1];
        int[] newPartitionsSet2 = new int[partitions2.length + 1];
        System.arraycopy(partitions1, 0, newPartitionsSet1, 0, partitions1.length - 1);
        System.arraycopy(partitions2, 0, newPartitionsSet2, 0, partitions2.length);
        newPartitionsSet2[partitions2.length] = partitions1[partitions1.length - 1];

        Map<Address, int[]> newAssignedPartitions = new HashMap<>();
        newAssignedPartitions.put(address1, newPartitionsSet1);
        newAssignedPartitions.put(address2, newPartitionsSet2);

        processor.init(
                new TestOutbox(1),
                new TestProcessorContext()
                        .setHazelcastInstance(instance1)
                        .setPartitionAssignment(newAssignedPartitions)
                        .setJobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
        );
        List<MapIndexScanP.Split> split = processor.splitOnMigration(0);
        assertEquals(2, split.size());
        assertFalse(processor.complete());
    }


    @Ignore
    @Test
    public void testConcurrentMigrationHandling() {
        List<Object[]> expected = new ArrayList<>();
        for (int i = itemsCount; i > 0; i--) {
            map.put(i, new Person("value-" + i, i));
            expected.add(new Object[]{(itemsCount - i + 1), "value-" + (itemsCount - i + 1), (itemsCount - i + 1)});
        }

        IndexConfig indexConfig = new IndexConfig(IndexType.SORTED, "age");
        indexConfig.setName(randomName());
        map.addIndex(indexConfig);

        IndexFilter filter = new IndexRangeFilter(intValue(0), true, intValue(itemsCount), true);
        List<Expression<?>> projections = asList(
                ColumnExpression.create(0, INT),
                ColumnExpression.create(1, VARCHAR),
                ColumnExpression.create(2, INT)
        );

        MapIndexScanMetadata scanMetadata = new MapIndexScanMetadata(
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

        TestProcessorMetaSupplierContext metaSupplierContext = new TestProcessorMetaSupplierContext();
        metaSupplierContext.setHazelcastInstance(instance1);

        ProcessorMetaSupplier metaSupplier = adaptSupplier(MapIndexScanP.readMapIndexSupplier(scanMetadata));
        TestProcessorSupplierContext psContext = new TestProcessorSupplierContext()
                .setHazelcastInstance(instance1)
                .setJobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()));
        Processor processor = TestSupport.supplierFrom(metaSupplier, psContext).get();

        assertFalse(processor.complete());

        MapProxyImpl<Integer, Person> mapProxy = (MapProxyImpl<Integer, Person>) map;
        int migrationStamp = mapProxy.getService().getMigrationStamp();

        // TODO
//        HazelcastInstance newInstance = FACTORY.newHazelcastInstance(smallInstanceConfig());
//        try {
//            // Await for migration stamp to change.
//            assertTrueEventually(() -> assertNotEquals(migrationStamp, mapProxy.getService().getMigrationStamp()));
//
//            // Try advance, should catch an exception
//            while (!processor.complete()) ;
//            assertTrue(processor.complete());
//
//        } finally {
//            newInstance.shutdown();
//        }
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

    private static IndexFilterValue intValue(Integer value) {
        return intValue(value, false);
    }

    private static IndexFilterValue intValue(Integer value, boolean allowNull) {
        return new IndexFilterValue(
                singletonList(constant(value, QueryDataType.INT)),
                singletonList(allowNull)
        );
    }

    private static ConstantExpression constant(Object value, QueryDataType type) {
        return ConstantExpression.create(value, type);
    }
}
