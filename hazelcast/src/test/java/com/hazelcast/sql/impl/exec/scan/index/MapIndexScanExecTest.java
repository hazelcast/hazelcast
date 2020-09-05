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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.scan.AbstractMapScanExec;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexScanExecTest extends SqlTestSupport {

    private static final int BATCH_SIZE = AbstractMapScanExec.BATCH_SIZE;
    private static final int PARTITION_COUNT = 10;

    private static final String MAP_NAME = "map";
    private static final String INDEX_NAME = "index";

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void beforeClass() {
        instance1 = factory.newHazelcastInstance(getInstanceConfig());
        instance2 = factory.newHazelcastInstance(getInstanceConfig());

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(IndexType.SORTED).addAttribute("this"));
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testScan() {
        checkScan(
            instance1,
            0,
            getLocalPartitions(instance1),
            null,
            null,
            Collections.emptyList(),
            1,
            new int[0]
        );

        int entryCount = BATCH_SIZE * 5 / 2;

        checkScan(
            instance1,
            entryCount,
            getLocalPartitions(instance1),
            null,
            null,
            Collections.emptyList(),
            1,
            IntStream.range(0, entryCount).toArray()
        );
    }

    @Test
    public void testIndexFilter() {
        int entryCount = 100;
        int from = 50;

        IndexFilterValue fromValue = new IndexFilterValue(
            Collections.singletonList(ConstantExpression.create(from, QueryDataType.INT)),
            Collections.singletonList(true)
        );

        IndexFilter indexFilter = new IndexRangeFilter(fromValue, true, null, false);

        checkScan(
            instance1,
            entryCount,
            getLocalPartitions(instance1),
            indexFilter,
            null,
            Collections.emptyList(),
            1,
            IntStream.range(from, entryCount).toArray()
        );
    }

    @Test
    public void testIndexFilterAndRemainderFilter() {
        int entryCount = 100;
        int from = 50;
        int to = 75;

        IndexFilterValue fromValue = new IndexFilterValue(
            Collections.singletonList(ConstantExpression.create(from, QueryDataType.INT)),
            Collections.singletonList(true)
        );

        IndexFilter indexFilter = new IndexRangeFilter(fromValue, true, null, false);

        Expression<Boolean> remainderFilter = ComparisonPredicate.create(
            ColumnExpression.create(0, QueryDataType.INT),
            ConstantExpression.create(75, QueryDataType.INT),
            ComparisonMode.LESS_THAN
        );

        checkScan(
            instance1,
            entryCount,
            getLocalPartitions(instance1),
            indexFilter,
            remainderFilter,
            Collections.emptyList(),
            1,
            IntStream.range(from, to).toArray()
        );
    }

    @Test
    public void testInvalidComponentCount() {
        IndexEqualsFilter indexFilter = new IndexEqualsFilter(
            new IndexFilterValue(
                Collections.singletonList(ConstantExpression.create(1, QueryDataType.INT)),
                Collections.singletonList(true)
            )
        );

        try {
            checkScan(
                instance1,
                0,
                getLocalPartitions(instance1),
                indexFilter,
                null,
                Arrays.asList(QueryDataType.INT, QueryDataType.INT),
                2,
                new int[0]
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" because it has 1 component(s), but 2 expected", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }
    }

    @Test
    public void testConverterProblems() {
        // Converters are not checked for scans.
        checkScan(
            instance1,
            0,
            getLocalPartitions(instance1),
            null,
            null,
            Collections.singletonList(QueryDataType.VARCHAR),
            1,
            new int[0]
        );

        // Converted must be checked for lookups
        IndexEqualsFilter indexFilter = new IndexEqualsFilter(
            new IndexFilterValue(
                Collections.singletonList(ConstantExpression.create(1, QueryDataType.INT)),
                Collections.singletonList(true)
            )
        );

        // Check missing converter (i.e. no data).
        try {
            checkScan(
                instance1,
                0,
                getLocalPartitions(instance1),
                indexFilter,
                null,
                Collections.singletonList(QueryDataType.INT),
                1,
                new int[0]
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" because it does not have suitable converter for component \"this\" (expected INTEGER)", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }

        // Check converter mismatch (i.e. data differs!).
        try {
            checkScan(
                instance1,
                1,
                getLocalPartitions(instance1),
                indexFilter,
                null,
                Collections.singletonList(QueryDataType.VARCHAR),
                1,
                new int[0]
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" because it has component \"this\" of type INTEGER, but VARCHAR was expected", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }
    }

    @Test
    public void testPartition_setup() {
        try {
            checkScan(
                instance1,
                1,
                getLocalPartitions(instance2),
                null,
                null,
                Collections.singletonList(QueryDataType.INT),
                1,
                new int[0]
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" due to concurrent migration, or because index creation is still in progress", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }
    }

    @Test
    public void testPartition_exec() {
        PartitionIdSet expectedPartitions = getLocalPartitions(instance1);

        IndexFilter indexFilter = new IndexFilter() {
            @SuppressWarnings("rawtypes")
            @Override
            public Iterator<QueryableEntry> getEntries(InternalIndex index, ExpressionEvalContext evalContext) {
                // Preserve the original stamp
                long stamp = index.getPartitionStamp(expectedPartitions);
                assertNotEquals(GlobalIndexPartitionTracker.STAMP_INVALID, stamp);

                // Kill the other member to trigger migrations
                instance2.shutdown();

                // Wait for stamp to change
                assertTrueEventually(() -> assertNotEquals(stamp, index.getPartitionStamp(expectedPartitions)));

                // Proceed
                return index.getSqlRecordIterator();
            }

            @SuppressWarnings("rawtypes")
            @Override
            public Comparable getComparable(ExpressionEvalContext evalContext) {
                return null;
            }
        };

        try {
            checkScan(
                instance1,
                1,
                expectedPartitions,
                indexFilter,
                null,
                Collections.singletonList(QueryDataType.INT),
                1,
                new int[0]
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" due to concurrent migration, or because index creation is still in progress", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }
    }

    @Test
    public void testNoIndex() {
        try {
            checkScan(
                instance1,
                "bad_index",
                0,
                getLocalPartitions(instance1),
                null,
                null,
                Arrays.asList(QueryDataType.INT, QueryDataType.INT),
                2,
                new int[0]
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"bad_index\" of the IMap \"map\" because it doesn't exist", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }
    }

    private void checkScan(
        HazelcastInstance member,
        int entryCount,
        PartitionIdSet partitions,
        IndexFilter indexFilter,
        Expression<Boolean> remainderFilter,
        List<QueryDataType> converterTypes,
        int expectedComponentCount,
        int[] expectedResults
    ) {
        checkScan(
            member,
            INDEX_NAME,
            entryCount,
            partitions,
            indexFilter,
            remainderFilter,
            converterTypes,
            expectedComponentCount,
            expectedResults
        );
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void checkScan(
        HazelcastInstance member,
        String indexName,
        int entryCount,
        PartitionIdSet partitions,
        IndexFilter indexFilter,
        Expression<Boolean> remainderFilter,
        List<QueryDataType> converterTypes,
        int expectedComponentCount,
        int[] expectedResults
    ) {
        IMap<Integer, Integer> map = member.getMap(MAP_NAME);

        Map<Integer, Integer> localEntries = getLocalEntries(member, entryCount, i -> i, i -> i);
        map.putAll(localEntries);

        List<QueryPath> fieldPaths = Collections.singletonList(valuePath(null));
        List<QueryDataType> fieldTypes = Collections.singletonList(QueryDataType.INT);
        List<Integer> projects = Collections.singletonList(0);

        MapIndexScanExec exec = new MapIndexScanExec(
            1,
            getMapContainer(map),
            partitions,
            GenericQueryTargetDescriptor.DEFAULT,
            GenericQueryTargetDescriptor.DEFAULT,
            fieldPaths,
            fieldTypes,
            projects,
            remainderFilter,
            getSerializationService(member),
            indexName,
            expectedComponentCount,
            indexFilter,
            converterTypes
        );

        exec.setup(emptyFragmentContext());

        List<Integer> results = new ArrayList<>();

        while (true) {
            IterationResult iterationResult = exec.advance();

            RowBatch batch = exec.currentBatch();

            for (int i = 0; i < batch.getRowCount(); i++) {
                Integer result = batch.getRow(i).get(0);

                results.add(result);
            }

            if (iterationResult == IterationResult.FETCHED_DONE) {
                break;
            }
        }

        List<Integer> expectedResults0 = new ArrayList<>(expectedResults.length);

        for (int expectedResult : expectedResults) {
            expectedResults0.add(expectedResult);
        }

        results.sort(Integer::compareTo);
        expectedResults0.sort(Integer::compareTo);
    }

    private static Config getInstanceConfig() {
        return new Config().setProperty("hazelcast.partition.count", Integer.toString(PARTITION_COUNT));
    }
}
