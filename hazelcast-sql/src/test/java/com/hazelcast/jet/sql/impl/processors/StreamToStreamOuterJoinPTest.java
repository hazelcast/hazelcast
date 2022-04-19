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
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class StreamToStreamOuterJoinPTest extends SimpleTestInClusterSupport {
    private static final Expression<Boolean> ODD_PREDICATE = ComparisonPredicate.create(
            RemainderFunction.create(
                    ColumnExpression.create(0, BIGINT),
                    ConstantExpression.create(2, BIGINT),
                    BIGINT),
            ConstantExpression.create(0, BIGINT),
            ComparisonMode.NOT_EQUALS
    );

    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> leftExtractors = singletonMap((byte) 0, l -> l.getRow().get(0));
    private final Map<Byte, ToLongFunctionEx<JetSqlRow>> rightExtractors = singletonMap((byte) 1, r -> r.getRow().get(0));
    private Map<Byte, Map<Byte, Long>> postponeTimeMap;
    private JetJoinInfo joinInfo;

    @Before
    public void before() {
        //noinspection unchecked
        postponeTimeMap = new HashMap<>();
        joinInfo = new JetJoinInfo(
                JoinRelType.LEFT,
                new int[]{0},
                new int[]{0},
                null,
                ODD_PREDICATE
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
                                jetRow(11L, 11L),
                                jetRow(12L, 13L),
                                wm((byte) 0, 13L)
                        ),
                        asList(
                                jetRow(9L),
                                wm((byte) 1, 15L),
                                jetRow(16L),
                                wm((byte) 1, 16)
                        )
                ))
                .expectOutput(
                        asList(
                                jetRow(11L, 11L, null),
                                wm((byte) 0, 13L)
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
