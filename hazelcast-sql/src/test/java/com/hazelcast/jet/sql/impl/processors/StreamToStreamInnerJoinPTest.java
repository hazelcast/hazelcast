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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.math.MinusFunction;
import com.hazelcast.sql.impl.expression.math.RemainderFunction;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.TestContextSupport.adaptSupplier;
import static com.hazelcast.jet.core.test.TestSupport.SAME_ITEMS_ANY_ORDER;
import static com.hazelcast.jet.core.test.TestSupport.in;
import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.sql.impl.expression.ExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.calcite.rel.core.JoinRelType.INNER;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class StreamToStreamInnerJoinPTest extends SimpleTestInClusterSupport {
    private static final Expression<Boolean> ODD_PREDICATE = ComparisonPredicate.create(
            RemainderFunction.create(
                    ColumnExpression.create(0, BIGINT),
                    ConstantExpression.create(2, BIGINT),
                    BIGINT),
            ConstantExpression.create(0, BIGINT),
            ComparisonMode.NOT_EQUALS
    );

    private Map<Byte, ToLongFunctionEx<JetSqlRow>> leftExtractors = singletonMap((byte) 0, l -> l.getRow().get(0));
    private Map<Byte, ToLongFunctionEx<JetSqlRow>> rightExtractors = singletonMap((byte) 1, r -> r.getRow().get(0));
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap = new HashMap<>();
    private JetJoinInfo joinInfo;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void before() {
        joinInfo = new JetJoinInfo(
                INNER,
                new int[]{0},
                new int[]{0},
                null,
                ComparisonPredicate.create(
                        ColumnExpression.create(0, BIGINT),
                        ColumnExpression.create(1, BIGINT),
                        ComparisonMode.EQUALS));
    }

    @Test
    public void test_equalTimes_singleWmKeyPerInput() {
        // l.time=r.time
        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 0L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 0L));

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(1, 1));

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
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

//    @Test
//    public void when_equalTimesConditionAndSingleWmKeyPerInput_then_eventsRemovedInTime() {
//        // l.time=r.time
//        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 0L));
//        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 0L));
//
//        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
//                joinInfo,
//                leftExtractors,
//                rightExtractors,
//                postponeTimeMap,
//                Tuple2.tuple2(1, 1));
//
//        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
//                .hazelcastInstance(instance())
//                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
//                .disableSnapshots()
//                .expectExactOutput(
//                        in(0, jetRow(0L)),
//                        in(1, jetRow(0L)),
//                        out(jetRow(0L, 0L)),
//                        in(0, wm((byte) 0, 1L)),
//                        out(wm((byte) 0, 0L)),
//                        in(1, wm((byte) 1, 1L)),
//                        out(wm((byte) 1, 1L)),
//                        out(wm((byte) 0, 1L)),
//                        in(0, jetRow(2L)),
//                        in(1, jetRow(2L)),
//                        out(jetRow(2L, 2L)),
//                        in(0, wm((byte) 0, 3L)),
//                        out(wm((byte) 0, 2L)),
//                        in(1, wm((byte) 1, 3L)),
//                        out(wm((byte) 1, 3L)),
//                        out(wm((byte) 0, 3L)),
//                        in(0, jetRow(4L)),
//                        in(1, jetRow(4L)),
//                        out(jetRow(4L, 4L)),
//                        in(0, wm((byte) 0, 5L)),
//                        out(wm((byte) 0, 4L)),
//                        in(1, wm((byte) 1, 5L)),
//                        out(wm((byte) 1, 5L)),
//                        out(wm((byte) 0, 5L))
//                );
//    }

    @Test
    public void given_alwaysTrueCondition_when_twoWmKeysOnLeftAndSingleKeyWmRightInput_then_successful() {
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

        Expression<Boolean> condition = createConditionFromPostponeTimeMap(postponeTimeMap);
        joinInfo = new JetJoinInfo(INNER, new int[0], new int[0], condition, condition);

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(2, 1));

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(SAME_ITEMS_ANY_ORDER)
                .disableSnapshots()
                .expectExactOutput(
                        in(0, jetRow(12L, 9L)),
                        in(1, jetRow(9L)),
                        out(jetRow(12L, 9L, 9L)),
                        in(0, jetRow(12L, 13L)),
                        out(jetRow(12L, 13L, 9L)),
                        // TODO actually assert the buffer state
                        // leftBuffer=[{12, 9}, {12, 13}]
                        // rightBuffer=[{9}]
                        in(1, wm((byte) 2, 15L)),
                        // leftBuffer=[{12, 13}]
                        // rightBuffer=[{9}]
                        out(wm((byte) 2, 9L)),
                        in(0, wm((byte) 1, 12L)),
                        // leftBuffer=[{12, 13}]
                        // rightBuffer=[]
                        out(wm((byte) 1, 12L)),
                        out(wm((byte) 2, 15L)),
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

        Expression<Boolean> condition = createConditionFromPostponeTimeMap(postponeTimeMap);
        joinInfo = new JetJoinInfo(INNER, new int[0], new int[0], condition, condition);

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(2, 2));

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
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
    public void given_oddNumbersFilter_when_twoWmKeysOnLeftAndSingleKeyWmRightInput_then_successful() {
        // left ordinal
        postponeTimeMap.put((byte) 0, emptyMap());
        postponeTimeMap.put((byte) 1, singletonMap((byte) 2, 1L));
        // right ordinal
        postponeTimeMap.put((byte) 2, singletonMap((byte) 1, 4L));

        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));
        rightExtractors = singletonMap((byte) 2, r -> r.getRow().get(0));

        joinInfo = new JetJoinInfo(
                INNER,
                new int[]{0},
                new int[]{0},
                null,
                ODD_PREDICATE
        );

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(2, 1));

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(11L, 11L),
                                jetRow(12L, 13L),
                                wm((byte) 0, 13L),
                                wm((byte) 1, 13L)
                        ),
                        asList(
                                jetRow(9L),
                                wm((byte) 2, 15L),
                                jetRow(16L),
                                wm((byte) 2, 16)
                        )
                ))
                .expectOutput(
                        asList(
                                jetRow(11L, 11L, 9L),
                                // MIN = min(15, 15-4) = 11
                                wm((byte) 2, 11L),
                                // MIN = 11 (min element)
                                wm((byte) 0, 11L),
                                jetRow(11L, 11L, 16L),
                                // MIN = min(13, 13-1) = 12
                                wm((byte) 1, 12L),
                                // MIN = min(16, 16-4) = 12
                                wm((byte) 2, 12L)
                        )
                );
    }

    @Test
    public void given_oddNumbersFilter_when_threeWmKeysOnLeftAndSingleKeyWmRightInput_then_successful() {
        // region
        // left ordinal
        postponeTimeMap.put((byte) 0, emptyMap());
        postponeTimeMap.put((byte) 1, emptyMap());
        postponeTimeMap.put((byte) 2, emptyMap());
        // right ordinal
        postponeTimeMap.put((byte) 3, ImmutableMap.of((byte) 1, 2L, (byte) 2, 2L));

        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));
        leftExtractors.put((byte) 2, l -> l.getRow().get(2));
        rightExtractors = singletonMap((byte) 3, r -> r.getRow().get(0));

        joinInfo = new JetJoinInfo(
                INNER,
                new int[]{0},
                new int[]{0},
                null,
                ODD_PREDICATE
        );

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(3, 1));

        // endregion

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hazelcastInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(2L, 2L, 2L),
                                jetRow(3L, 3L, 3L),
                                wm((byte) 0, 3L),
                                wm((byte) 1, 3L),
                                wm((byte) 2, 3L),
                                jetRow(5L, 5L, 5L)
                        ),
                        asList(
                                jetRow(2L),
                                wm((byte) 3, 3L),
                                wm((byte) 3, 3L),
                                wm((byte) 3, 3L),
                                wm((byte) 3, 3L),
                                jetRow(4L)
                        )
                ))
                .expectOutput(
                        asList(
                                jetRow(3L, 3L, 3L, 2L),
                                wm((byte) 3, 1L),
                                // minimum in buffer -> 1
                                wm((byte) 0, 2L),
                                wm((byte) 1, 2L),
                                // 1 was removed, minimum in buffer -> 3
                                wm((byte) 2, 2L),
                                // no wm(t > 3) was produced,
                                // so (5,5,5,2) is valid here.
                                jetRow(5L, 5L, 5L, 2L),
                                // (1,1,1) and (2,2,2) were removed.
                                jetRow(3L, 3L, 3L, 4L),
                                jetRow(5L, 5L, 5L, 4L)
                        )
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
     */
    private static Expression<Boolean> createConditionFromPostponeTimeMap(Map<Byte, Map<Byte, Long>> postponeTimeMap) {
        // we assume that the WM key `n` is the column `n` in the joined row
        List<Expression<Boolean>> conditions = new ArrayList<>();
        for (Entry<Byte, Map<Byte, Long>> enOuter : postponeTimeMap.entrySet()) {
            for (Entry<Byte, Long> enInner : enOuter.getValue().entrySet()) {
                conditions.add(ComparisonPredicate.create(
                        ColumnExpression.create(enOuter.getKey(), BIGINT),
                        MinusFunction.create(
                                ColumnExpression.create(enInner.getKey(), BIGINT),
                                ConstantExpression.create(enInner.getValue(), BIGINT),
                                BIGINT),
                        ComparisonMode.GREATER_THAN_OR_EQUAL));
            }
        }

        return AndPredicate.create(conditions.toArray(new Expression[0]));
    }
}
