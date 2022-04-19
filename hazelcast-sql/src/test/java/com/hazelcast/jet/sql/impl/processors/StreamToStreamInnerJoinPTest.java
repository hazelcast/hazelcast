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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class StreamToStreamInnerJoinPTest extends SimpleTestInClusterSupport {
    private static final ConstantExpression<?> TRUE_PREDICATE = ConstantExpression.create(true, BOOLEAN);
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
    private Map<Byte, Map<Byte, Long>> postponeTimeMap;
    private JetJoinInfo joinInfo;

    @Before
    public void before() {
        //noinspection unchecked
        postponeTimeMap = new HashMap<>();
        joinInfo = new JetJoinInfo(
                JoinRelType.INNER,
                new int[]{0},
                new int[]{0},
                null,
                (Expression<Boolean>) TRUE_PREDICATE
        );
    }

    @Test
    public void given_alwaysTrueCondition_when_singleWmKeyPerInput_then_successful() {
        postponeTimeMap.put((byte) 0, singletonMap((byte) 0, 0L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 1, 0L));

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(0L),
                                jetRow(1L),
                                wm((byte) 0, 1L)
                        ),
                        asList(
                                jetRow(0L),
                                jetRow(1L),
                                wm((byte) 1, 1L)
                        )
                ))
                .expectOutput(
                        asList(
                                // left <- 0
                                // right <- 0
                                jetRow(0L, 0L),
                                // left <- 1
                                jetRow(1L, 0L),
                                // right <- 1
                                jetRow(0L, 1L),
                                jetRow(1L, 1L),
                                wm((byte) 0, 1L),
                                wm((byte) 1, 1L)
                        )
                );
    }

    @Test
    public void given_alwaysTrueCondition_when_twoWmKeysOnLeftAndSingleKeyWmRightInput_then_successful() {
        // left ordinal
        postponeTimeMap.put((byte) 0, singletonMap((byte) 0, 0L));
        postponeTimeMap.put((byte) 1, mapOf((byte) 1, 0L, (byte) 2, 1L));
        // right ordinal
        postponeTimeMap.put((byte) 2, mapOf((byte) 1, 4L, (byte) 2, 0L));

        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));
        rightExtractors = singletonMap((byte) 2, r -> r.getRow().get(0));

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(12L, 10L),
                                jetRow(12L, 13L),
                                wm((byte) 1, 12L),
                                wm((byte) 0, 13L),
                                wm((byte) 1, 13L)
                        ),
                        asList(
                                jetRow(9L),
                                wm((byte) 2, 15L),
                                jetRow(16L),
                                jetRow(16L)
                        )
                ))
                .expectOutput(
                        asList(
                                // left <- (12, 10)
                                // right <- (9)
                                jetRow(12L, 10L, 9L),
                                // left <- (12, 13)
                                jetRow(12L, 13L, 9L),
                                // left <-  wm(1, 12)
                                // right <- wm(2, 15)
                                wm((byte) 2, 9L),
                                wm((byte) 1, 13L), // new minimum for key=1 is 13, so emit the freshest WM.
                                // right <- (21)
                                jetRow(12L, 13L, 16L)
                                // wm((byte) 0, 12L) // TODO: expected, but not reached ?
                        )
                );
    }

    /*
        SELECT * FROM a
        JOIN b ON b.a BETWEEN a.a AND a.a + 1
        JOIN c ON c.a BETWEEN b.a AND b.a + 2  -- 'c' contains WMs '2' and '3'.
     */
    @Test
    public void given_alwaysTrueCondition_when_twoWmKeysOnBothInputs_then_successful() {
        // left ordinal
        postponeTimeMap.put((byte) 0, singletonMap((byte) 0, 0L));
        postponeTimeMap.put((byte) 1, mapOf((byte) 0, 1L, (byte) 1, 0L));
        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));

        // right ordinal
        postponeTimeMap.put((byte) 2, mapOf((byte) 1, 2L, (byte) 2, 0L));
        postponeTimeMap.put((byte) 3, mapOf((byte) 1, 2L, (byte) 3, 0L));
        rightExtractors = new HashMap<>();
        rightExtractors.put((byte) 2, r -> r.getRow().get(0));
        rightExtractors.put((byte) 3, r -> r.getRow().get(1));

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(12L, 10L),
                                jetRow(12L, 13L),
                                wm((byte) 1, 12L),
                                wm((byte) 0, 13L)
                        ),
                        asList(
                                jetRow(12L, 10L),
                                jetRow(12L, 13L),
                                wm((byte) 3, 12L),
                                wm((byte) 2, 13L)
                        )
                ))
                .expectOutput(
                        asList(
                                // left <- (12, 10)
                                // right <- (9)
                                jetRow(12L, 10L, 12L, 10L),
                                jetRow(12L, 13L, 12L, 10L),
                                jetRow(12L, 10L, 12L, 13L),
                                jetRow(12L, 13L, 12L, 13L),
                                wm((byte) 1, 10L),
                                wm((byte) 3, 10L),
                                wm((byte) 0, 12L),
                                wm((byte) 2, 12L)
                        )
                );
    }

    @Test
    public void given_oddNumbersFilter_when_twoWmKeysOnLeftAndSingleKeyWmRightInput_then_successful() {
        // left ordinal
        postponeTimeMap.put((byte) 0, singletonMap((byte) 0, 0L));
        postponeTimeMap.put((byte) 1, mapOf((byte) 1, 0L, (byte) 2, 1L));
        // right ordinal
        postponeTimeMap.put((byte) 2, mapOf((byte) 1, 4L, (byte) 2, 0L));

        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));
        rightExtractors = singletonMap((byte) 2, r -> r.getRow().get(0));

        joinInfo = new JetJoinInfo(
                JoinRelType.INNER,
                new int[]{0},
                new int[]{0},
                null,
                ODD_PREDICATE
        );

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(11L, 11L),
                                jetRow(12L, 13L),
                                wm((byte) 1, 12L),
                                wm((byte) 0, 13L),
                                wm((byte) 1, 13L)
                        ),
                        asList(
                                jetRow(9L),
                                wm((byte) 2, 15L),
                                jetRow(16L),
                                jetRow(16L),
                                wm((byte) 2, 16)
                        )
                ))
                .expectOutput(
                        asList(
                                jetRow(11L, 11L, 9L),
                                wm((byte) 2, 9L),
                                wm((byte) 1, 11L),
                                jetRow(11L, 11L, 16L),
                                wm((byte) 2, 16L)
                                // right <- (21)
                                // wm((byte) 0, 12L) // TODO: expected, but not reached ?
                        )
                );
    }

    @Test
    public void given_oddNumbersFilter_when_threeWmKeysOnLeftAndSingleKeyWmRightInput_then_successful() {
        // region
        // left ordinal
        postponeTimeMap.put((byte) 0, singletonMap((byte) 0, 0L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 1, 0L));
        postponeTimeMap.put((byte) 2, singletonMap((byte) 2, 0L));
        // right ordinal
        postponeTimeMap.put((byte) 3, mapOf((byte) 1, 2L, (byte) 2, 2L, (byte) 3, 0L));

        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));
        leftExtractors.put((byte) 2, l -> l.getRow().get(2));
        rightExtractors = singletonMap((byte) 3, r -> r.getRow().get(0));

        joinInfo = new JetJoinInfo(
                JoinRelType.INNER,
                new int[]{0},
                new int[]{0},
                null,
                ODD_PREDICATE
        );

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap);

        // endregion

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .disableProgressAssertion()
                .inputs(asList(
                        asList(
                                jetRow(1L, 1L, 1L),
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
                                wm((byte) 3, 3L),
                                jetRow(4L)
                        )
                ))
                .expectOutput(
                        asList(
                                jetRow(1L, 1L, 1L, 2L),
                                wm((byte) 3, 2L),
                                jetRow(3L, 3L, 3L, 2L),
                                wm((byte) 3, 2L),
                                wm((byte) 0, 3L),
                                wm((byte) 3, 2L),
                                wm((byte) 1, 3L),
                                wm((byte) 3, 2L),
                                wm((byte) 2, 3L),
                                wm((byte) 3, 2L),
                                // no wm(t > 3) was produced,
                                // so (5,5,5,2) is valid here.
                                jetRow(5L, 5L, 5L, 2L),
                                // (1,1,1) and (2,2,2) were removed.
                                jetRow(3L, 3L, 3L, 4L),
                                jetRow(5L, 5L, 5L, 4L)
                        )
                );
    }

    static Map<Byte, Long> mapOf(Byte key1, Long value1, Byte key2, Long value2) {
        Map<Byte, Long> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    static Map<Byte, Long> mapOf(Byte key1, Long value1, Byte key2, Long value2, Byte key3, Long value3) {
        Map<Byte, Long> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        return map;
    }
}
