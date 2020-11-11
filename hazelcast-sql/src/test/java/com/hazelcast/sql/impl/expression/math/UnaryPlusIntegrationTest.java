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
import static com.hazelcast.sql.SqlColumnType.TINYINT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnaryPlusIntegrationTest extends ExpressionIntegrationTestBase {

    @Test
    public void testBoolean() {
        assertParsingError("+booleanTrue", "No operator matches '+<BOOLEAN>' name and argument types (you might need to add an explicit CAST)");
    }

    @Test
    public void testTinyint() {
        assertRow("+byte0", EXPR0, TINYINT, (byte) 0);
        assertRow("+byte1", EXPR0, TINYINT, (byte) 1);
        assertRow("+byteMax", EXPR0, TINYINT, Byte.MAX_VALUE);
        assertRow("+byteMin", EXPR0, TINYINT, Byte.MIN_VALUE);
    }

    @Test
    public void testSmallint() {
        assertRow("+short0", EXPR0, SMALLINT, (short) 0);
        assertRow("+short1", EXPR0, SMALLINT, (short) 1);
        assertRow("+shortMax", EXPR0, SMALLINT, Short.MAX_VALUE);
        assertRow("+shortMin", EXPR0, SMALLINT, Short.MIN_VALUE);
    }

    @Test
    public void testInteger() {
        assertRow("+int0", EXPR0, INTEGER, 0);
        assertRow("+int1", EXPR0, INTEGER, 1);
        assertRow("+intMax", EXPR0, INTEGER, Integer.MAX_VALUE);
        assertRow("+intMin", EXPR0, INTEGER, Integer.MIN_VALUE);
    }

    @Test
    public void testBigint() {
        assertRow("+long0", EXPR0, BIGINT, 0L);
        assertRow("+long1", EXPR0, BIGINT, 1L);
        assertRow("+longMax", EXPR0, BIGINT, Long.MAX_VALUE);
        assertRow("+longMin", EXPR0, BIGINT, Long.MIN_VALUE);
    }

    @Test
    public void testReal() {
        assertRow("+float0", EXPR0, REAL, 0.0f);
        assertRow("+float1", EXPR0, REAL, 1.0f);
        assertRow("+floatMax", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("+floatMin", EXPR0, REAL, Float.MIN_VALUE);
    }

    @Test
    public void testDouble() {
        assertRow("+double0", EXPR0, DOUBLE, 0.0);
        assertRow("+double1", EXPR0, DOUBLE, 1.0);
        assertRow("+doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);
        assertRow("+doubleMin", EXPR0, DOUBLE, Double.MIN_VALUE);
    }

    @Test
    public void testDecimal() {
        assertRow("+decimal0", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("+decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("+decimalBig", EXPR0, DECIMAL, getRecord().decimalBig);
        assertRow("+decimalBigNegative", EXPR0, DECIMAL, getRecord().decimalBigNegative);

        assertRow("+bigInteger0", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("+bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("+bigIntegerBig", EXPR0, DECIMAL, getRecord().decimalBig);
        assertRow("+bigIntegerBigNegative", EXPR0, DECIMAL, getRecord().decimalBigNegative);
    }

    @Test
    public void testVarchar() {
        assertRow("+string0", EXPR0, DOUBLE, 0.0);
        assertRow("+string1", EXPR0, DOUBLE, 1.0);
        assertRow("+stringBig", EXPR0, DOUBLE, Double.parseDouble(getRecord().stringBig));
        assertRow("+stringBigNegative", EXPR0, DOUBLE, Double.parseDouble(getRecord().stringBigNegative));
        assertDataError("+stringFoo", "Cannot convert VARCHAR to DOUBLE");

        assertRow("+char0", EXPR0, DOUBLE, 0.0);
        assertRow("+char1", EXPR0, DOUBLE, 1.0);
        assertDataError("+charF", "Cannot convert VARCHAR to DOUBLE");
    }

    @Test
    public void testUnsupported() {
        assertParsingError("+dateCol", "No operator matches '+<DATE>' name and argument types (you might need to add an explicit CAST)");
        assertParsingError("+timeCol", "No operator matches '+<TIME>' name and argument types (you might need to add an explicit CAST)");
        assertParsingError("+dateTimeCol", "No operator matches '+<TIMESTAMP>' name and argument types (you might need to add an explicit CAST)");
        assertParsingError("+offsetDateTimeCol", "No operator matches '+<TIMESTAMP_WITH_TIME_ZONE>' name and argument types (you might need to add an explicit CAST)");
        assertParsingError("+object", "No operator matches '+<OBJECT>' name and argument types (you might need to add an explicit CAST)");
    }
}
