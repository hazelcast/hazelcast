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

package com.hazelcast.jet.sql.impl.processors;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.math.MinusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.core.test.TestSupport.SAME_ITEMS_ANY_ORDER;
import static com.hazelcast.jet.core.test.TestSupport.in;
import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.core.test.TestSupport.processorAssertion;
import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class StreamToStreamJoinPInnerTest extends SimpleTestInClusterSupport {

    private Map<Byte, ToLongFunctionEx<JetSqlRow>> leftExtractors = singletonMap((byte) 0, l -> l.getRow().get(0));
    private Map<Byte, ToLongFunctionEx<JetSqlRow>> rightExtractors = singletonMap((byte) 1, r -> r.getRow().get(0));
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap = new HashMap<>();

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void test_equalTimes_singleWmKeyPerInput() {
        // l.time=r.time
        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 0L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 0L));
        ProcessorSupplier processorSupplier = ProcessorSupplier.of(createProcessor(1, 1));

        TestSupport.verifyProcessor(adaptSupplier(processorSupplier))
                .hazelcastInstance(instance())
                .disableSnapshots()
                .outputChecker(TestSupport.SAME_ITEMS_ANY_ORDER)
                .expectExactOutput(
                        in(0, jetRow(0L)),
                        in(1, jetRow(0L)),
                        out(jetRow(0L, 0L)),
                        in(0, jetRow(1L)),
                        in(1, jetRow(1L)),
                        out(jetRow(1L, 1L)),
                        in(wm((byte) 0, 1L)),
                        out(wm((byte) 0, 0L)),
                        in(wm((byte) 1, 1L)),
                        out(wm((byte) 0, 1L)),
                        out(wm((byte) 1, 1L))
                );
    }

    @Test
    public void test_twoWmKeysOnLeft() {
        // l.time2 BETWEEN r.time - 1 AND r.time + 4  (l.time1 irrelevant)
        // left ordinal
        postponeTimeMap.put((byte) 0, emptyMap());
        postponeTimeMap.put((byte) 1, singletonMap((byte) 2, 1L));
        // right ordinal
        postponeTimeMap.put((byte) 2, singletonMap((byte) 1, 4L));

        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));
        rightExtractors = singletonMap((byte) 2, r -> r.getRow().get(0));

        SupplierEx<Processor> supplier = createProcessor(2, 1);

        TestSupport.verifyProcessor(supplier)
                .hazelcastInstance(instance())
                .outputChecker(SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .expectExactOutput(
                        in(0, jetRow(12L, 9L)),
                        in(1, jetRow(9L)),
                        out(jetRow(12L, 9L, 9L)),
                        in(0, jetRow(12L, 13L)),
                        out(jetRow(12L, 13L, 9L)),
                        processorAssertion((StreamToStreamJoinP p) -> {
                            assertEquals(asList(jetRow(12L, 9L), jetRow(12L, 13L)), p.buffer[0]);
                            assertEquals(singletonList(jetRow(9L)), p.buffer[1]);
                        }),
                        in(1, wm((byte) 2, 15L)),
                        out(wm((byte) 2, 9L)),
                        processorAssertion((StreamToStreamJoinP p) -> {
                            assertEquals(singletonList(jetRow(12L, 13L)), p.buffer[0]);
                            assertEquals(singletonList(jetRow(9L)), p.buffer[1]);
                        }),
                        in(0, wm((byte) 1, 12L)),
                        out(wm((byte) 1, 12L)),
                        out(wm((byte) 2, 15L)),
                        processorAssertion((StreamToStreamJoinP p) -> {
                            assertEquals(singletonList(jetRow(12L, 13L)), p.buffer[0]);
                            assertEquals(emptyList(), p.buffer[1]);
                        }),
                        in(0, wm((byte) 0, 13L)),
                        out(wm((byte) 0, 12L)),
                        in(1, jetRow(16L)),
                        // out(jetRow(12L, 13L, 16L)), // doesn't satisfy the join condition
                        in(0, wm((byte) 1, 13L)),
                        out(wm((byte) 1, 13L)),
                        in(1, jetRow(16L))
                );
    }

    @Test
    public void test_twoWmKeysOnEachInput() {
        // `l` and `r` both have `time1` and `time2` columns
        // Join condition:
        //     r.time1 BETWEEN l.time1 -1 AND l.time1 + 2
        //     AND r.time2 BETWEEN l.time1 - 3 AND l.time1 + 4

        // left ordinal
        postponeTimeMap.put((byte) 0, ImmutableMap.of((byte) 2, 2L, (byte) 3, 4L));
        postponeTimeMap.put((byte) 1, ImmutableMap.of());
        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));

        // right ordinal
        postponeTimeMap.put((byte) 2, ImmutableMap.of((byte) 0, 1L));
        postponeTimeMap.put((byte) 3, ImmutableMap.of((byte) 0, 3L));
        rightExtractors = new HashMap<>();
        rightExtractors.put((byte) 2, r -> r.getRow().get(0));
        rightExtractors.put((byte) 3, r -> r.getRow().get(1));

        SupplierEx<Processor> supplier = createProcessor(2, 2);

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .disableSnapshots()
                .outputChecker(SAME_ITEMS_ANY_ORDER)
                .expectExactOutput(
                        in(0, jetRow(12L, 10L)),
                        in(1, jetRow(12L, 10L)),
                        out(jetRow(12L, 10L, 12L, 10L)),
                        in(0, jetRow(12L, 13L)),
                        out(jetRow(12L, 13L, 12L, 10L)),
                        in(1, jetRow(12L, 13L)),
                        out(jetRow(12L, 10L, 12L, 13L)),
                        out(jetRow(12L, 13L, 12L, 13L)),
                        // leftBuffer: [{12, 10}, {12, 13}]
                        // rightBuffer: [{12, 10}, {12, 13}]
                        in(0, wm((byte) 0, 15L)),
                        // wmState: [{0:min, 1:min, 2:13, 3:11]
                        // leftBuffer: [{12, 10}, {12, 13}]
                        // rightBuffer: []
                        out(wm((byte) 0, 12L)),
                        in(1, wm((byte) 2, 15L)),
                        // wmState: [{0:14, 1:min, 2:13, 3:11]
                        // leftBuffer: []
                        // rightBuffer: []
                        out(wm((byte) 0, 15L)),
                        out(wm((byte) 2, 15L)),
                        in(0, wm((byte) 1, 16L)),
                        out(wm((byte) 1, 16L))
                );
    }

    @Test
    public void test_joinWithAdditionalCondition() {
        // Join condition:
        //    r.time BETWEEN l.time - 1 and l.time + 1 AND l.field1 > 10
        // l's columns (in this order): `time, field1`

        postponeTimeMap.put((byte) 0, ImmutableMap.of((byte) 1, 1L));
        postponeTimeMap.put((byte) 1, ImmutableMap.of((byte) 0, 1L));

        leftExtractors = ImmutableMap.of((byte) 0, l -> l.getRow().get(0));
        rightExtractors = ImmutableMap.of((byte) 1, r -> r.getRow().get(0));

        Expression<Boolean> condition = AndPredicate.create(
                createConditionFromPostponeTimeMap(postponeTimeMap, 1, 2),
                ComparisonPredicate.create(
                        ColumnExpression.create(1, QueryDataType.INT),
                        ConstantExpression.create(10, QueryDataType.INT),
                        ComparisonMode.GREATER_THAN));
        JetJoinInfo joinInfo = new JetJoinInfo(INNER, new int[0], new int[0], condition, condition);

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(2, 1));

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .disableSnapshots()
                .outputChecker(SAME_ITEMS_ANY_ORDER)
                .expectExactOutput(
                        in(1, jetRow(3L)),
                        in(0, jetRow(2L, 2)), // doesn't join, field1 <= 10
                        in(0, jetRow(2L, 42)), // joins, field1 > 10
                        out(jetRow(2L, 42, 3L)));
    }

    private SupplierEx<Processor> createProcessor(int leftColumnCount, int rightColumnCount) {
        Expression<Boolean> condition = createConditionFromPostponeTimeMap(postponeTimeMap);
        JetJoinInfo joinInfo = new JetJoinInfo(INNER, new int[0], new int[0], condition, condition);
        return () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(leftColumnCount, rightColumnCount));
    }

    @Test
    public void test_nonLateItemOutOfLimit() {
        // Join condition:
        //     l.time BETWEEN r.time - 1 AND r.time + 1
        postponeTimeMap.put((byte) 0, ImmutableMap.of((byte) 1, 1L));
        postponeTimeMap.put((byte) 1, ImmutableMap.of((byte) 0, 1L));

        leftExtractors = singletonMap((byte) 0, l -> l.getRow().get(0));
        rightExtractors = singletonMap((byte) 1, r -> r.getRow().get(0));

        SupplierEx<Processor> supplier = createProcessor(1, 1);

        TestSupport.verifyProcessor(supplier)
                .hazelcastInstance(instance())
                .outputChecker(SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .expectExactOutput(
                        in(0, wm((byte) 0, 10)),
                        processorAssertion((StreamToStreamJoinP p) -> {
                            assertEquals(ImmutableMap.of((byte) 0, Long.MIN_VALUE + 1, (byte) 1, 9L), p.wmState);
                        }),
                        // This item is not late according to the WM for key=1, but is off the join limit according
                        // to the WM for key=1 and the postponing time
                        in(1, jetRow(8L)),
                        processorAssertion((StreamToStreamJoinP p) -> {
                            assertEquals(0, p.buffer[1].size());
                        })
                );
    }

    @Test
    public void test_dropLateItems() {
        // Join condition:
        //     l.time BETWEEN r.time - 1 AND r.time + 1
        postponeTimeMap.put((byte) 0, ImmutableMap.of((byte) 1, 1L));
        postponeTimeMap.put((byte) 1, ImmutableMap.of((byte) 0, 1L));

        leftExtractors = singletonMap((byte) 0, l -> l.getRow().get(0));
        rightExtractors = singletonMap((byte) 1, r -> r.getRow().get(0));

        SupplierEx<Processor> supplier = createProcessor(1, 1);

        TestSupport.verifyProcessor(supplier)
                .hazelcastInstance(instance())
                .outputChecker(SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .expectExactOutput(
                        in(0, wm((byte) 0, 10)),
                        in(0, jetRow(8L)),
                        processorAssertion((StreamToStreamJoinP p) -> {
                            assertEquals(0, p.buffer[0].size());
                            assertEquals(0, p.buffer[1].size());
                        })
                );
    }

    /**
     * From the postponeTimeMap create the equivalent condition for the join processor.
     *
     * For example, this join condition (`l` has `time1` and `time2` columns, `r` has `time`):
     *     l.time2 BETWEEN r.time - 1 AND r.time + 4
     *
     * Is transformed to:
     *     l.time2 >= r.time - 1
     *     r.time >= l.time2 - 4
     *
     * For which this postponeTimeMap is created:
     *     0: {}
     *     1: {2:4}
     *     2: {1:1}
     *
     * @param wmKeyToColumnIndex Remapping of WM keys to joined column indexes. Contains
     *                           a sequence of `wmKey1`, `index1`, `wmKey2`, `index2, ... If WM key == index,
     *                           no entry is needed.
     */
    private static Expression<Boolean> createConditionFromPostponeTimeMap(
            Map<Byte, Map<Byte, Long>> postponeTimeMap,
            int... wmKeyToColumnIndex
    ) {
        Map<Byte, Byte> wmKeyToColumnIndexMap = new HashMap<>();
        for (int i = 0; i < wmKeyToColumnIndex.length; i += 2) {
            wmKeyToColumnIndexMap.put((byte) wmKeyToColumnIndex[i], (byte) wmKeyToColumnIndex[i + 1]);
        }

        List<Expression<Boolean>> conditions = new ArrayList<>();
        for (Entry<Byte, Map<Byte, Long>> enOuter : postponeTimeMap.entrySet()) {
            for (Entry<Byte, Long> enInner : enOuter.getValue().entrySet()) {
                int leftColumnIndex = wmKeyToColumnIndexMap.getOrDefault(enOuter.getKey(), enOuter.getKey());
                int rightColumnIndex = wmKeyToColumnIndexMap.getOrDefault(enInner.getKey(), enInner.getKey());
                conditions.add(ComparisonPredicate.create(
                        ColumnExpression.create(leftColumnIndex, BIGINT),
                        MinusFunction.create(
                                ColumnExpression.create(rightColumnIndex, BIGINT),
                                ConstantExpression.create(enInner.getValue(), BIGINT),
                                BIGINT),
                        ComparisonMode.GREATER_THAN_OR_EQUAL));
            }
        }

        return AndPredicate.create(conditions.toArray(new Expression[0]));
    }
}
