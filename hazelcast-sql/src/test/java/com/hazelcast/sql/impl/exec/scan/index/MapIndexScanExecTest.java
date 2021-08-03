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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker.PartitionStamp;
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
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexScanExecTest extends SqlTestSupport {

    private static final int BATCH_SIZE = AbstractMapScanExec.BATCH_SIZE;
    private static final int PARTITION_COUNT = 10;

    private static final String MAP_NAME = "map";
    private static final String INDEX_NAME = "index";

    @Parameterized.Parameters(name = "ascending:{0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Parameterized.Parameter
    public boolean ascending;

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void before() {
        instance1 = factory.newHazelcastInstance(getInstanceConfig());
        instance2 = factory.newHazelcastInstance(getInstanceConfig());

        assertClusterSizeEventually(2, instance1, instance2);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.addIndex(new IndexConfig().setName(INDEX_NAME).setType(IndexType.SORTED).addAttribute("this"));

        waitAllForSafeState(instance1, instance2);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testScan() {
        List<Integer> entriesForEmptyMap = executeScan(
                instance1,
                getLocalPartitions(instance1),
                null,
                null,
                Collections.emptyList(),
                1
        );

        assertTrue(entriesForEmptyMap.isEmpty());

        int entryCount = BATCH_SIZE * 5 / 2;

        populate(instance1, entryCount);

        List<Integer> entries1 = executeScan(
                instance1,
                getLocalPartitions(instance1),
                null,
                null,
                Collections.emptyList(),
                1
        );

        List<Integer> entries2 = executeScan(
                instance2,
                getLocalPartitions(instance2),
                null,
                null,
                Collections.emptyList(),
                1
        );

        checkValues(IntStream.range(0, entryCount).toArray(), entries1, entries2);
    }

    @Test
    public void testIndexFilter() {
        int entryCount = 100;
        int from = 50;

        populate(instance1, entryCount);

        IndexFilterValue fromValue = new IndexFilterValue(
                Collections.singletonList(ConstantExpression.create(from, QueryDataType.INT)),
                Collections.singletonList(true)
        );

        IndexFilter indexFilter = new IndexRangeFilter(fromValue, true, null, false);

        List<Integer> entries1 = executeScan(
                instance1,
                getLocalPartitions(instance1),
                indexFilter,
                null,
                Collections.emptyList(),
                1
        );

        List<Integer> entries2 = executeScan(
                instance2,
                getLocalPartitions(instance2),
                indexFilter,
                null,
                Collections.emptyList(),
                1
        );

        checkValues(IntStream.range(from, entryCount).toArray(), entries1, entries2);
    }

    @Test
    public void testIndexFilterAndRemainderFilter() {
        int entryCount = 100;
        int from = 50;
        int to = 75;

        populate(instance1, entryCount);

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

        List<Integer> entries1 = executeScan(
                instance1,
                getLocalPartitions(instance1),
                indexFilter,
                remainderFilter,
                Collections.emptyList(),
                1
        );

        List<Integer> entries2 = executeScan(
                instance2,
                getLocalPartitions(instance2),
                indexFilter,
                remainderFilter,
                Collections.emptyList(),
                1
        );

        checkValues(IntStream.range(from, to).toArray(), entries1, entries2);
    }

    private void checkValues(int[] expectedValues, List<Integer> entriesLeft, List<Integer> entriesRight) {
        assertEquals(expectedValues.length, entriesLeft.size() + entriesRight.size());

        if (!ascending) {
            int[] expectedValuesReversed = new int[expectedValues.length];

            for (int i = 0; i < expectedValues.length; i++) {
                expectedValuesReversed[expectedValues.length - i - 1] = expectedValues[i];
            }

            expectedValues = expectedValuesReversed;
        }

        int[] actualValues = new int[expectedValues.length];

        int leftPos = 0;
        int rightPos = 0;

        for (int i = 0; i < actualValues.length; i++) {
            boolean takeLeft;

            if (entriesRight.size() == rightPos) {
                // Right input is over.
                takeLeft = true;
            } else if (entriesLeft.size() == leftPos) {
                // Left input is over.
                takeLeft = false;
            } else {
                int left = entriesLeft.get(leftPos);
                int right = entriesRight.get(rightPos);

                takeLeft = ascending ? left < right : left > right;
            }

            if (takeLeft) {
                actualValues[i] = entriesLeft.get(leftPos++);
            } else {
                actualValues[i] = entriesRight.get(rightPos++);
            }
        }

        assertArrayEquals(expectedValues, actualValues);
    }

    @Test
    public void testInvalidComponentCount() {
        populate(instance1, 100);

        IndexEqualsFilter indexFilter = new IndexEqualsFilter(
                new IndexFilterValue(
                        Collections.singletonList(ConstantExpression.create(1, QueryDataType.INT)),
                        Collections.singletonList(true)
                )
        );

        try {
            executeScan(
                    instance1,
                    getLocalPartitions(instance1),
                    indexFilter,
                    null,
                    Arrays.asList(QueryDataType.INT, QueryDataType.INT),
                    2
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" because it has 1 component(s), but 2 expected", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }
    }

    @Test
    public void testNoConverterCheckOnScan() {
        populate(instance1, 100);

        executeScan(
                instance1,
                getLocalPartitions(instance1),
                null,
                null,
                Collections.singletonList(QueryDataType.VARCHAR),
                1
        );
    }

    @Test
    public void testConverterErrorOnEmptyMap() {
        IndexEqualsFilter indexFilter = new IndexEqualsFilter(
                new IndexFilterValue(
                        Collections.singletonList(ConstantExpression.create(1, QueryDataType.INT)),
                        Collections.singletonList(true)
                )
        );

        // Check missing converter (i.e. no data).
        try {
            executeScan(
                    instance1,
                    getLocalPartitions(instance1),
                    indexFilter,
                    null,
                    Collections.singletonList(QueryDataType.INT),
                    1
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" because it does not have suitable converter for component \"this\" (expected INTEGER)", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }
    }

    @Test
    public void testConverterMismatch() {
        populate(instance1, 100);

        IndexEqualsFilter indexFilter = new IndexEqualsFilter(
                new IndexFilterValue(
                        Collections.singletonList(ConstantExpression.create(1, QueryDataType.INT)),
                        Collections.singletonList(true)
                )
        );

        // Check converter mismatch (i.e. data differs!).
        try {
            executeScan(
                    instance1,
                    getLocalPartitions(instance1),
                    indexFilter,
                    null,
                    Collections.singletonList(QueryDataType.VARCHAR),
                    1
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
            populate(instance1, 100);

            executeScan(
                    instance1,
                    getLocalPartitions(instance2),
                    null,
                    null,
                    Collections.singletonList(QueryDataType.INT),
                    1
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
            public Iterator<QueryableEntry> getEntries(InternalIndex index, boolean descending, ExpressionEvalContext evalContext) {
                // Preserve the original stamp
                PartitionStamp stamp = index.getPartitionStamp();
                assertNotNull(stamp);
                assertEquals(expectedPartitions, stamp.partitions);

                // Kill the other member to trigger migrations
                instance2.shutdown();

                // Wait for stamp to change
                assertTrueEventually(() -> assertNotEquals(stamp, index.getPartitionStamp()));

                // Proceed
                return index.getSqlRecordIterator(descending);
            }

            @SuppressWarnings("rawtypes")
            @Override
            public Comparable getComparable(ExpressionEvalContext evalContext) {
                return null;
            }
        };

        try {
            populate(instance1, 100);

            executeScan(
                    instance1,
                    expectedPartitions,
                    indexFilter,
                    null,
                    Collections.singletonList(QueryDataType.INT),
                    1
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
            executeScan(
                    instance1,
                    "bad_index",
                    getLocalPartitions(instance1),
                    null,
                    null,
                    Arrays.asList(QueryDataType.INT, QueryDataType.INT),
                    2
            );

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            assertEquals("Cannot use the index \"bad_index\" of the IMap \"map\" because it doesn't exist", e.getMessage());
            assertTrue(e.isInvalidatePlan());
        }
    }

    private List<Integer> executeScan(
            HazelcastInstance member,
            PartitionIdSet partitions,
            IndexFilter indexFilter,
            Expression<Boolean> remainderFilter,
            List<QueryDataType> converterTypes,
            int expectedComponentCount
    ) {
        return executeScan(
                member,
                INDEX_NAME,
                partitions,
                indexFilter,
                remainderFilter,
                converterTypes,
                expectedComponentCount
        );
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private List<Integer> executeScan(
            HazelcastInstance member,
            String indexName,
            PartitionIdSet partitions,
            IndexFilter indexFilter,
            Expression<Boolean> remainderFilter,
            List<QueryDataType> converterTypes,
            int expectedComponentCount
    ) {
        List<QueryPath> fieldPaths = Collections.singletonList(valuePath(null));
        List<QueryDataType> fieldTypes = Collections.singletonList(QueryDataType.INT);
        List<Integer> projects = Collections.singletonList(0);
        List<Boolean> ascs = Collections.singletonList(ascending);

        MapIndexScanExec exec = new MapIndexScanExec(
                1,
                getMapContainer(member.getMap(MAP_NAME)),
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
                converterTypes,
                ascs
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

        return results;
    }

    private void populate(HazelcastInstance member, int entryCount) {
        Map<Integer, Integer> entries = new HashMap<>();

        for (int i = 0; i < entryCount; i++) {
            entries.put(i, i);
        }

        member.getMap(MAP_NAME).putAll(entries);
    }

    private static Config getInstanceConfig() {
        return new Config().setProperty("hazelcast.partition.count", Integer.toString(PARTITION_COUNT));
    }
}
