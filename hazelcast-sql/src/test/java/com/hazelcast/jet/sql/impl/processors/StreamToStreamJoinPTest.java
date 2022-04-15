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
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
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
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class StreamToStreamJoinPTest extends SimpleTestInClusterSupport {
    private static final Expression<Boolean> TRUE_PREDICATE =
            (Expression<Boolean>) ConstantExpression.create(true, BOOLEAN);

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
                TRUE_PREDICATE
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
                                wm((byte) 0, 0L),
                                wm((byte) 1, 0L)
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

    static Map<Byte, Long> mapOf(Byte key1, Long value1, Byte key2, Long value2) {
        Map<Byte, Long> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }
}
