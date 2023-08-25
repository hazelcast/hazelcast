/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.opt.physical.StreamToStreamJoinPhysicalRule.tryExtractTimeBound;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.CAST;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.EQUALS;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.GREATER_THAN;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.GREATER_THAN_OR_EQUAL;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.LESS_THAN;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.LESS_THAN_OR_EQUAL;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.MINUS;
import static com.hazelcast.jet.sql.impl.validate.HazelcastSqlOperatorTable.PLUS;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.junit.Assert.assertEquals;

public class StreamToStreamJoinPhysicalRuleTest extends OptimizerTestSupport {
    @Test
    public void test_maxWindowSize() {
        Map<Integer, Map<Integer, Long>> postponeTimeMap = new HashMap<>();
        postponeTimeMap.put(0, singletonMap(3, 10L));
        postponeTimeMap.put(1, singletonMap(4, 10L));
        postponeTimeMap.put(2, singletonMap(5, 15L));
        postponeTimeMap.put(3, singletonMap(0, 6L));
        postponeTimeMap.put(4, singletonMap(1, 13L));
        postponeTimeMap.put(5, singletonMap(2, 6L));

        assertEquals(16L, StreamToStreamJoinPhysicalRel.minimumSpread(postponeTimeMap, 3));
    }

    @Test
    public void test() {
        HazelcastTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;
        RexBuilder b = new RexBuilder(typeFactory);
        RexInputRef leftTime = b.makeInputRef(typeFactory.createSqlType(TIMESTAMP_WITH_LOCAL_TIME_ZONE), 0);
        RexInputRef rightTime = b.makeInputRef(typeFactory.createSqlType(TIMESTAMP_WITH_LOCAL_TIME_ZONE), 2);
        RexLiteral intervalTen = b.makeIntervalLiteral(BigDecimal.TEN,
                new SqlIntervalQualifier(TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO));
        RexLiteral intervalZero = b.makeIntervalLiteral(BigDecimal.ZERO,
                new SqlIntervalQualifier(TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO));

        // leftTime >= rightTime
        assertEquals(ImmutableMap.of(2, ImmutableMap.of(0, 0L)),
                call(b.makeCall(GREATER_THAN_OR_EQUAL, leftTime, rightTime)));
        // leftTime <= rightTime + 10 ==> rightTime >= leftTime - 10
        assertEquals(ImmutableMap.of(0, ImmutableMap.of(2, 10L)),
                call(b.makeCall(LESS_THAN_OR_EQUAL, leftTime, b.makeCall(PLUS, rightTime, intervalTen))));

        // leftTime - rightTime < 10 ==> rightTime > leftTime - 10
        assertEquals(ImmutableMap.of(0, ImmutableMap.of(2, 10L)),
                call(b.makeCall(LESS_THAN, b.makeCall(MINUS, leftTime, rightTime), intervalTen)));

        // leftTime - rightTime + 10 < 0 ==> rightTime > leftTime + 10
        assertEquals(ImmutableMap.of(0, ImmutableMap.of(2, -10L)),
                call(b.makeCall(LESS_THAN,
                        b.makeCall(PLUS,
                                b.makeCall(MINUS, leftTime, rightTime),
                                intervalTen),
                        intervalZero)));

        // leftTime + 10 > rightTime + 10 - 10 ==> leftTime > rightTime - 10
        assertEquals(ImmutableMap.of(2, ImmutableMap.of(0, 10L)),
                call(b.makeCall(GREATER_THAN,
                        b.makeCall(PLUS, leftTime, intervalTen),
                        b.makeCall(MINUS,
                                b.makeCall(PLUS, rightTime, intervalTen),
                                intervalTen))));

        // leftTime = rightTime + 10 ==> leftTime >= rightTime + 10 AND rightTime >= leftTime - 10
        assertEquals(ImmutableMap.of(0, ImmutableMap.of(2, +10L), 2, ImmutableMap.of(0, -10L)),
                call(b.makeCall(EQUALS, leftTime, b.makeCall(PLUS, rightTime, intervalTen))));

        assertEquals(emptyMap(), call(intervalTen));
    }

    @Test
    public void test_implicitCasts() {
        HazelcastTypeFactory typeFactory = HazelcastTypeFactory.INSTANCE;
        RexBuilder b = new RexBuilder(typeFactory);
        RexInputRef leftTime = b.makeInputRef(typeFactory.createSqlType(TINYINT), 0);
        RexInputRef rightTime = b.makeInputRef(typeFactory.createSqlType(TINYINT), 2);

        // CAST(leftTime AS BIGINT) == CAST(rightTime AS BIGINT)
        assertEquals(ImmutableMap.of(2, ImmutableMap.of(0, 0L)),
                call(b.makeCall(GREATER_THAN_OR_EQUAL,
                        b.makeCall(typeFactory.createSqlType(BIGINT), CAST, singletonList(leftTime)),
                        b.makeCall(typeFactory.createSqlType(BIGINT), CAST, singletonList(rightTime))
                )));
    }

    public Map<Integer, Map<Integer, Long>> call(RexNode expr) {
        Map<Integer, Map<Integer, Long>> map = new HashMap<>();
        tryExtractTimeBound(expr, ImmutableSet.of(0, 2), map);
        return map;
    }
}
