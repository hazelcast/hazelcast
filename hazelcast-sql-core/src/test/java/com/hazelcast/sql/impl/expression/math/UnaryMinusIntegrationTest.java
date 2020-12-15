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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.impl.expression.ExpressionIntegrationTestBase;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnaryMinusIntegrationTest extends ExpressionIntegrationTestBase {

    @Test
    public void testBoolean() {
        assertParsingError("-booleanTrue", "Cannot apply '-' to arguments of type '-<BOOLEAN>'");
    }

    @Test
    public void testTinyint() {
        assertRow("-byte0", EXPR0, SMALLINT, (short) 0);
        assertRow("-byte1", EXPR0, SMALLINT, (short) -1);
        assertRow("-byteMax", EXPR0, SMALLINT, (short) -Byte.MAX_VALUE);
        assertRow("-byteMin", EXPR0, SMALLINT, (short) -Byte.MIN_VALUE);
    }

    @Test
    public void testSmallint() {
        assertRow("-short0", EXPR0, INTEGER, 0);
        assertRow("-short1", EXPR0, INTEGER, -1);
        assertRow("-shortMax", EXPR0, INTEGER, -Short.MAX_VALUE);
        assertRow("-shortMin", EXPR0, INTEGER, -Short.MIN_VALUE);
    }

    @Test
    public void testInteger() {
        assertRow("-int0", EXPR0, BIGINT, 0L);
        assertRow("-int1", EXPR0, BIGINT, -1L);
        assertRow("-intMax", EXPR0, BIGINT, -(long) Integer.MAX_VALUE);
        assertRow("-intMin", EXPR0, BIGINT, -(long) Integer.MIN_VALUE);
    }

    @Test
    public void testBigint() {
        assertRow("-long0", EXPR0, BIGINT, 0L);
        assertRow("-long1", EXPR0, BIGINT, -1L);
        assertRow("-longMax", EXPR0, BIGINT, -Long.MAX_VALUE);
        assertDataError("-longMin", "BIGINT overflow");
    }

    @Test
    public void testReal() {
        assertRow("-float0", EXPR0, REAL, -0.0f);
        assertRow("-float1", EXPR0, REAL, -1.0f);
        assertRow("-floatMax", EXPR0, REAL, -Float.MAX_VALUE);
        assertRow("-floatMin", EXPR0, REAL, -Float.MIN_VALUE);
    }

    @Test
    public void testDouble() {
        assertRow("-double0", EXPR0, DOUBLE, -0.0);
        assertRow("-double1", EXPR0, DOUBLE, -1.0);
        assertRow("-doubleMax", EXPR0, DOUBLE, -Double.MAX_VALUE);
        assertRow("-doubleMin", EXPR0, DOUBLE, -Double.MIN_VALUE);
    }

    @Test
    public void testDecimal() {
        assertRow("-decimal0", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("-decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(-1));
        assertRow("-decimalBig", EXPR0, DECIMAL, getRecord().decimalBig.negate());
        assertRow("-decimalBigNegative", EXPR0, DECIMAL, getRecord().decimalBigNegative.negate());

        assertRow("-bigInteger0", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("-bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(-1));
        assertRow("-bigIntegerBig", EXPR0, DECIMAL, getRecord().decimalBig.negate());
        assertRow("-bigIntegerBigNegative", EXPR0, DECIMAL, getRecord().decimalBigNegative.negate());

    }

    @Test
    public void testVarchar() {
        assertRow("-string0", EXPR0, DOUBLE, -0.0);
        assertRow("-string1", EXPR0, DOUBLE, -1.0);
        assertRow("-stringBig", EXPR0, DOUBLE, -Double.parseDouble(getRecord().stringBig));
        assertRow("-stringBigNegative", EXPR0, DOUBLE, -Double.parseDouble(getRecord().stringBigNegative));
        assertDataError("-stringFoo", "Cannot convert VARCHAR to DOUBLE");

        assertRow("-char0", EXPR0, DOUBLE, -0.0);
        assertRow("-char1", EXPR0, DOUBLE, -1.0);
        assertDataError("-charF", "Cannot convert VARCHAR to DOUBLE");
    }

    @Test
    public void testUnsupported() {
        assertParsingError("-dateCol", "Cannot apply '-' to arguments of type '-<DATE>'");
        assertParsingError("-timeCol", "Cannot apply '-' to arguments of type '-<TIME>'");
        assertParsingError("-dateTimeCol", "Cannot apply '-' to arguments of type '-<TIMESTAMP>'");
        assertParsingError("-offsetDateTimeCol", "Cannot apply '-' to arguments of type '-<TIMESTAMP_WITH_TIME_ZONE>'");
        assertParsingError("-object", "Cannot apply '-' to arguments of type '-<OBJECT>'");
    }

}
