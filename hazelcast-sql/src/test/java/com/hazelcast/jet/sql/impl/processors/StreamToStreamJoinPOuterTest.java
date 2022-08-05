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
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.core.test.TestSupport.in;
import static com.hazelcast.jet.core.test.TestSupport.out;
import static com.hazelcast.jet.core.test.TestSupport.processorAssertion;
import static com.hazelcast.jet.sql.SqlTestSupport.jetRow;
import static com.hazelcast.jet.sql.impl.processors.StreamToStreamJoinPInnerTest.createConditionFromPostponeTimeMap;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class StreamToStreamJoinPOuterTest extends JetTestSupport {

    private Map<Byte, ToLongFunctionEx<JetSqlRow>> leftExtractors = singletonMap((byte) 0, l -> l.getRow().get(0));
    private Map<Byte, ToLongFunctionEx<JetSqlRow>> rightExtractors = singletonMap((byte) 1, r -> r.getRow().get(0));
    private final Map<Byte, Map<Byte, Long>> postponeTimeMap = new HashMap<>();

    @Parameter
    public boolean isLeft;

    private byte ordinal0;
    private byte ordinal1;
    private JoinRelType joinType;

    @Parameters(name = "isLeft={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Before
    public void before() {
        if (isLeft) {
            ordinal0 = 0;
            ordinal1 = 1;
            joinType = JoinRelType.LEFT;
        } else {
            ordinal0 = 1;
            ordinal1 = 0;
            joinType = JoinRelType.RIGHT;
        }
    }

    @Test
    public void test_outerJoinRowsEmittedAfterWatermark() {
        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 1L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 1L));

        SupplierEx<Processor> supplier = createProcessor(1, 1);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .expectExactOutput(
                        in(0, wm(1L, (byte) 0)),
                        out(wm(1L, (byte) 0)),
                        in(1, wm(1L, (byte) 1)),
                        out(wm(1L, (byte) 1)),
                        in(ordinal0, jetRow(3L)),
                        in(ordinal0, jetRow(4L)),
                        in(ordinal1, wm(6L, ordinal1)),
                        out(isLeft ? jetRow(3L, null) : jetRow(null, 3L)),
                        out(isLeft ? jetRow(4L, null) : jetRow(null, 4L)),
                        out(wm(6L, ordinal1)),
                        processorAssertion((StreamToStreamJoinP p) ->
                                assertEquals(0, p.buffer[0].size() + p.buffer[1].size()))
                );
    }

    @Test
    public void test_rowContainsMultipleColumns() {
        postponeTimeMap.put((byte) 0, singletonMap((byte) 1, 0L));
        postponeTimeMap.put((byte) 1, singletonMap((byte) 0, 0L));

        TestSupport.verifyProcessor(isLeft ? createProcessor(1, 2) : createProcessor(2, 1))
                .disableSnapshots()
                .expectExactOutput(
                        in(0, wm(1L, (byte) 0)),
                        out(wm(1L, (byte) 0)),
                        in(1, wm(2L, (byte) 1)),
                        out(wm(2L, (byte) 1)),
                        in(ordinal0, jetRow(3L)),
                        in(ordinal0, jetRow(4L)),
                        in(ordinal1, wm(6L, ordinal1)),
                        out(isLeft ? jetRow(3L, null, null) : jetRow(null, null, 3L)),
                        out(isLeft ? jetRow(4L, null, null) : jetRow(null, null, 4L)),
                        out(wm(6L, ordinal1)),
                        processorAssertion((StreamToStreamJoinP p) ->
                                assertEquals(0, p.buffer[0].size() + p.buffer[1].size()))
                );
    }

    @Test
    public void test_nonLateItemOutOfLimit() {
        // Join condition:
        //     l.time BETWEEN r.time - 1 AND r.time + 1
        postponeTimeMap.put((byte) 0, ImmutableMap.of((byte) 1, 1L));
        postponeTimeMap.put((byte) 1, ImmutableMap.of((byte) 0, 1L));

        SupplierEx<Processor> supplier = createProcessor(1, 1);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .expectExactOutput(
                        in(ordinal1, wm(10, ordinal1)),
                        out(wm(10, ordinal1)),
                        processorAssertion((StreamToStreamJoinP p) ->
                                assertEquals(ImmutableMap.of(ordinal0, 9L, ordinal1, Long.MIN_VALUE + 1), p.wmState)),
                        // This item is not late according to the WM for key=1, but is off the join limit according
                        // to the WM for key=0, minus the postponing time
                        in(ordinal0, jetRow(8L)),
                        out(isLeft ? jetRow(8L, null) : jetRow(null, 8L)),
                        processorAssertion((StreamToStreamJoinP p) ->
                                assertEquals(0, p.buffer[0].size() + p.buffer[1].size()))
                );
    }

    @Test
    public void when_offLimitAccordingToWm1_and_lateAccordingToWm2_then_handleAsLate() {
        // l.time1 BETWEEN r.time - 1 AND r.time + 1  (l.time2 is irrelevant)
        // left ordinal
        postponeTimeMap.put((byte) 0, singletonMap((byte) 2, 1L));
        postponeTimeMap.put((byte) 1, emptyMap());
        // right ordinal
        postponeTimeMap.put((byte) 2, singletonMap((byte) 0, 1L));

        leftExtractors = new HashMap<>();
        leftExtractors.put((byte) 0, l -> l.getRow().get(0));
        leftExtractors.put((byte) 1, l -> l.getRow().get(1));
        rightExtractors = singletonMap((byte) 2, r -> r.getRow().get(0));

        SupplierEx<Processor> supplier = createProcessor(2, 1);

        TestSupport.verifyProcessor(supplier)
                .disableSnapshots()
                .expectExactOutput(
                        in(1, wm(10, (byte) 2)),
                        out(wm(10, (byte) 2)),
                        in(0, wm(10, (byte) 1)),
                        out(wm(10, (byte) 1)),
                        in(0, jetRow(0L, 0L))
                );
    }

    private SupplierEx<Processor> createProcessor(int leftColumnCount, int rightColumnCount) {
        Expression<Boolean> condition = createConditionFromPostponeTimeMap(postponeTimeMap);
        JetJoinInfo joinInfo = new JetJoinInfo(joinType, new int[0], new int[0], condition, condition);
        return () -> new StreamToStreamJoinP(
                joinInfo,
                leftExtractors,
                rightExtractors,
                postponeTimeMap,
                Tuple2.tuple2(leftColumnCount, rightColumnCount));
    }
}
