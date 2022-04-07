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
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;

import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static java.util.Arrays.asList;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class StreamToStreamJoinPTest extends SimpleTestInClusterSupport {
    private static final Expression<Boolean> TRUE_PREDICATE =
            (Expression<Boolean>) ConstantExpression.create(true, BOOLEAN);

    private HashMap<Byte, Long>[] postponeTimeMap;
    private JetJoinInfo joinInfo;

    @Before
    public void before() {
        //noinspection unchecked
        postponeTimeMap = new HashMap[2];
        postponeTimeMap[0] = new HashMap<>();
        postponeTimeMap[1] = new HashMap<>();

        joinInfo = new JetJoinInfo(
                JoinRelType.INNER,
                new int[]{0},
                new int[]{0},
                null,
                TRUE_PREDICATE
        );
    }

    @Test
    public void simpleTest() {
        postponeTimeMap[0].put((byte) 0, 1L);
        postponeTimeMap[1].put((byte) 1, 1L);

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                l -> l.getRow().get(0),
                r -> r.getRow().get(0),
                postponeTimeMap);
        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
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
    public void testWatermarkCleanup() {
        postponeTimeMap[0].put((byte) 0, 2L);
        postponeTimeMap[1].put((byte) 1, 2L);

        SupplierEx<Processor> supplier = () -> new StreamToStreamJoinP(
                joinInfo,
                l -> l.getRow().get(0),
                r -> r.getRow().get(0),
                postponeTimeMap);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .inputs(asList(
                        asList(
                                jetRow(0L),
                                jetRow(1L),
                                wm((byte) 0, 1L),
                                wm((byte) 0, 3L),
                                jetRow(5L),
                                jetRow(6L),
                                wm((byte) 0, 6L)
                        ),
                        asList(
                                jetRow(0L),
                                jetRow(1L),
                                wm((byte) 1, 1L),
                                wm((byte) 1, 3L),
                                jetRow(5L),
                                jetRow(6L),
                                wm((byte) 1, 6L)
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
                                wm((byte) 1, 1L),
                                wm((byte) 0, 3L),
                                wm((byte) 1, 3L),
                                // left <- 5
                                // right <- 5
                                jetRow(5L, 5L),
                                // left <- 6
                                jetRow(6L, 5L),
                                // right <- 6
                                jetRow(5L, 6L),
                                jetRow(6L, 6L),
                                wm((byte) 0, 6L),
                                wm((byte) 1, 6L)
                        )
                );
    }
}
