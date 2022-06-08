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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.math.RemainderFunction;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.core.test.TestSupport.in;
import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.core.test.TestSupport.processorAssertion;
import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinPInnerTest.createConditionFromPostponeTimeMap;
import static com.hazelcast.sql.impl.expression.ExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class StreamToStreamJoinPOuterTest extends JetTestSupport {
    private static final Expression<Boolean> ODD_PREDICATE = ComparisonPredicate.create(
            RemainderFunction.create(
                    ColumnExpression.create(1, BIGINT),  // OK for both LEFT and RIGHT JOIN.
                    ConstantExpression.create(2, BIGINT),
                    BIGINT),
            ConstantExpression.create(0, BIGINT),
            ComparisonMode.NOT_EQUALS
    );

    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftExtractors = singletonMap((byte) 0, l -> l.getRow().get(0));
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightExtractors = singletonMap((byte) 1, r -> r.getRow().get(0));
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap = new HashMap<>();
    private JetJoinInfo joinInfo;

    @Test
    public void given_leftJoin_when_oppositeBufferIsEmpty_then_fillNulls() {
        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 1L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 1L));
        Expression<Boolean> condition = createConditionFromPostponeTimeMap(postponeTimeMap);
        joinInfo = new JetJoinInfo(
                JoinRelType.LEFT,
                new int[0],
                new int[0],
                condition,
                condition
        );

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(1, 2));

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .expectExactOutput(
                        in(0, wm((byte) 0, 1L)),
                        out(wm((byte) 0, 1L)),
                        in(1, wm((byte) 1, 1L)),
                        out(wm((byte) 1, 1L)),
                        in(1, jetRow(3L, 3L)),
                        in(1, jetRow(4L, 4L)),
                        in(0, wm((byte) 0, 6L)),
                        out(jetRow(null, 3L, 3L)),
                        out(jetRow(null, 4L, 4L)),
                        out(wm((byte) 0, 6L)),
                        processorAssertion((StreamToStreamJoinP p) ->
                                assertEquals(0, p.buffer[0].size() + p.buffer[1].size()))
                );
    }

    @Test
    public void given_leftJoin_when_rowContainsMultipleColumns_then_successful() {
        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 0L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 0L));
        joinInfo = new JetJoinInfo(
                JoinRelType.LEFT,
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
                Tuple2.tuple2(2, 2));

        TestSupport.verifyProcessor(supplier)
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(1L, 1L),
                                jetRow(2L, 2L),
                                jetRow(3L, 3L),
                                wm((byte) 0, 3L)
                        ),
                        asList(
                                jetRow(1L, 1L),
                                jetRow(2L, 2L),
                                jetRow(3L, 3L),
                                wm((byte) 1, 3L)
                        )
                ))
                .expectOutput(
                        asList(
                                jetRow(1L, 1L, 1L, 1L),
                                jetRow(1L, 1L, 2L, 2L),
                                jetRow(3L, 3L, 1L, 1L),
                                jetRow(3L, 3L, 2L, 2L),
                                jetRow(1L, 1L, 3L, 3L),
                                jetRow(3L, 3L, 3L, 3L),
                                wm((byte) 0, 3L),
                                wm((byte) 1, 3L)
                        )
                );
    }

    @Test
    public void given_rightJoin_when_oppositeBufferIsEmpty_then_fillNulls() {
        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 1L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 1L));
        joinInfo = new JetJoinInfo(
                JoinRelType.RIGHT,
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

        TestSupport.verifyProcessor(supplier)
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                wm((byte) 0, 1L),
                                jetRow(3L, 3L),
                                jetRow(4L, 4L),
                                jetRow(5L, 5L),
                                wm((byte) 0, 6L)
                        ),
                        singletonList(
                                wm((byte) 1, 1L)
                        )
                ))
                .expectOutput(
                        asList(
                                wm((byte) 0, 0L),
                                wm((byte) 1, 0L),
                                jetRow(3L, 3L, null),
                                jetRow(5L, 5L, null),
                                wm((byte) 0, 5L)
                        )
                );
    }

    @Test
    public void given_rightJoin_when_rowContainsMultipleColumns_then_successful() {
        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 0L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 0L));
        joinInfo = new JetJoinInfo(
                JoinRelType.RIGHT,
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
                Tuple2.tuple2(2, 2));

        TestSupport.verifyProcessor(supplier)
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(1L, 1L),
                                jetRow(2L, 2L),
                                jetRow(3L, 3L),
                                wm((byte) 0, 3L)
                        ),
                        asList(
                                jetRow(1L, 1L),
                                jetRow(2L, 2L),
                                jetRow(3L, 3L),
                                wm((byte) 1, 3L)
                        )
                ))
                .expectOutput(
                        asList(
                                // NOTE: first row contains NULL since test processor process left input first
                                jetRow(1L, 1L, 1L, 1L),
                                jetRow(1L, 1L, 2L, 2L),
                                jetRow(3L, 3L, 1L, 1L),
                                jetRow(3L, 3L, 2L, 2L),
                                jetRow(1L, 1L, 3L, 3L),
                                jetRow(3L, 3L, 3L, 3L),
                                wm((byte) 0, 3L),
                                wm((byte) 1, 3L)
                        )
                );
    }
}
