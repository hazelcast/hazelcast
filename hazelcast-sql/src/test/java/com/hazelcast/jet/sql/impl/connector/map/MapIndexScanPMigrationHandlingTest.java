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
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.impl.connector.AbstractIndexReader;
import com.hazelcast.jet.sql.impl.connector.map.MapIndexScanP.MapIndexScanProcessorSupplier;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MapFetchIndexOperationResult;
import com.hazelcast.map.impl.operation.MapFetchIndexOperation.MissingPartitionException;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.exec.scan.MapScanExecTest;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.plan.node.MapIndexScanMetadata;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.getHazelcastInstance;
import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.comparisonFn;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.spi.impl.InternalCompletableFuture.*;
import static com.hazelcast.sql.impl.SqlTestSupport.valuePath;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
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

        HazelcastInstance newInstance = FACTORY.newHazelcastInstance(smallInstanceConfig());
        try {
            // Await for migration stamp to change.
            assertTrueEventually(() -> assertNotEquals(migrationStamp, mapProxy.getService().getMigrationStamp()));

            // Try advance, should catch an exception
            while (!processor.complete()) ;
            assertTrue(processor.complete());

        } finally {
            newInstance.shutdown();
        }
    }

    static class MockReader extends AbstractIndexReader<
            InternalCompletableFuture<MapFetchIndexOperationResult>,
            MapFetchIndexOperationResult,
            QueryableEntry<?, ?>> {

        private HazelcastInstance hazelcastInstance;

        @Nonnull
        @Override
        public InternalCompletableFuture<MapFetchIndexOperationResult> readBatch(
                Address address,
                PartitionIdSet partitions,
                IndexIterationPointer[] pointers
        ) {
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
