/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import org.junit.Test;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

public class CaseOperationIntegrationTest extends ExpressionTestSupport {
    @Test
    public void dummyCase() {
        put(1);

        String sql = "select case when 1 = 1 then 1 else 2 end from map";

        checkValue0(sql, SqlColumnType.TINYINT, (byte) 1);
    }

    @Test
    public void testOnlyElse() {
        put(1);

        String sql = "select case else 2 end from map";

        checkFailure0(sql, SqlErrorCode.PARSING, "Encountered \"case else\"");
    }

    @Test
    public void useMapValue() {
        put(1);

        String sql = "select case when this = 1 then 10 end from map";

        checkValue0(sql, SqlColumnType.TINYINT, (byte) 10);
    }

    @Test
    public void multipleConditions() {
        put(1);

        String sql = "select case when this > 1 then 10 when this = 1 then 100 end from map";

        checkValue0(sql, SqlColumnType.TINYINT, (byte) 100);
    }

    @Test
    public void doesntMatchWhen_andNoElseBranch() {
        put(1);

        String sql = "select case when this > 1 then 10 end from map";

        checkValue0(sql, SqlColumnType.TINYINT, null);
    }

    @Test
    public void differentReturnTypes() {
        put(1);

        String sql = "select case when 1 = 1 then 1 when 2 = 2 then 1000000000 else 'some string' end from map";

        checkValue0(sql, SqlColumnType.BIGINT, (byte) 0);
    }

    @Test
    public void testEquality() {
        checkEquals(
                when1eq1_then1_else10(),
                when1eq1_then1_else10(),
                true);

        checkEquals(
                when1eq1_then1_else10(),
                when1eq10_then1_else10(),
                false);

        checkEquals(
                when1eq1_then1_else10(),
                when1eq1_then_someText_else_anotherText(),
                false
        );
    }

    @Test
    public void testSerialization() {
        CaseExpression<?> original = when1eq1_then1_else10();
        CaseExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CASE);

        checkEquals(original, restored, true);
    }

    private CaseExpression<?> when1eq1_then_someText_else_anotherText() {
        return CaseExpression.create(
                new Expression[]{
                        ComparisonPredicate.create(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT), ComparisonMode.EQUALS),
                        ConstantExpression.create("someText", VARCHAR),
                        ConstantExpression.create("anotherText", VARCHAR),
                }, VARCHAR);
    }

    private CaseExpression<?> when1eq1_then1_else10() {
        return CaseExpression.create(
                new Expression[]{
                        ComparisonPredicate.create(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT), ComparisonMode.EQUALS),
                        ConstantExpression.create(10, INT),
                        ConstantExpression.create(20, INT),
                }, INT);
    }

    private CaseExpression<?> when1eq10_then1_else10() {
        return CaseExpression.create(
                new Expression[]{
                        ComparisonPredicate.create(ConstantExpression.create(1, INT), ConstantExpression.create(10, INT), ComparisonMode.EQUALS),
                        ConstantExpression.create(10, INT),
                        ConstantExpression.create(20, INT),
                }, INT);
    }
}
