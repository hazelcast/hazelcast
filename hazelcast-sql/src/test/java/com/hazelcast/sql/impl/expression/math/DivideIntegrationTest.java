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
public class DivideIntegrationTest extends ExpressionIntegrationTestBase {

    @Test
    public void testBoolean() {
        assertParsingError("booleanTrue / booleanTrue", "Cannot apply [BOOLEAN, BOOLEAN] to the '/' operator");

        assertParsingError("booleanTrue / byte1", "Cannot apply [BOOLEAN, TINYINT] to the '/' operator");
        assertParsingError("booleanTrue / short1", "Cannot apply [BOOLEAN, SMALLINT] to the '/' operator");
        assertParsingError("booleanTrue / int1", "Cannot apply [BOOLEAN, INTEGER] to the '/' operator");
        assertParsingError("booleanTrue / long1", "Cannot apply [BOOLEAN, BIGINT] to the '/' operator");

        assertParsingError("booleanTrue / float1", "Cannot apply [BOOLEAN, REAL] to the '/' operator");
        assertParsingError("booleanTrue / double1", "Cannot apply [BOOLEAN, DOUBLE] to the '/' operator");

        assertParsingError("booleanTrue / decimal1", "Cannot apply [BOOLEAN, DECIMAL] to the '/' operator");
        assertParsingError("booleanTrue / bigInteger1", "Cannot apply [BOOLEAN, DECIMAL] to the '/' operator");

        assertParsingError("booleanTrue / string1", "Cannot apply [BOOLEAN, VARCHAR] to the '/' operator");
        assertParsingError("booleanTrue / char1", "Cannot apply [BOOLEAN, VARCHAR] to the '/' operator");

        assertParsingError("booleanTrue / dateCol", "Cannot apply [BOOLEAN, DATE] to the '/' operator");
        assertParsingError("booleanTrue / timeCol", "Cannot apply [BOOLEAN, TIME] to the '/' operator");
        assertParsingError("booleanTrue / dateTimeCol", "Cannot apply [BOOLEAN, TIMESTAMP] to the '/' operator");
        assertParsingError("booleanTrue / offsetDateTimeCol", "Cannot apply [BOOLEAN, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("booleanTrue / object", "Cannot apply [BOOLEAN, OBJECT] to the '/' operator");
    }

    @Test
    public void testTinyint() {
        assertParsingError("byte1 / booleanTrue", "Cannot apply [TINYINT, BOOLEAN] to the '/' operator");

        assertRow("byte1 / byte2", EXPR0, TINYINT, (byte) 0);
        assertRow("byteMax / byte2", EXPR0, TINYINT, (byte) (Byte.MAX_VALUE / 2));
        assertDataError("byte1 / byte0", "division by zero");

        assertRow("byte1 / short2", EXPR0, SMALLINT, (short) 0);
        assertRow("byteMax / short2", EXPR0, SMALLINT, (short) (Byte.MAX_VALUE / 2));
        assertDataError("byte1 / short0", "division by zero");

        assertRow("byte1 / int2", EXPR0, INTEGER, 0);
        assertRow("byteMax / int2", EXPR0, INTEGER, Byte.MAX_VALUE / 2);
        assertDataError("byte1 / int0", "division by zero");

        assertRow("byte1 / long2", EXPR0, BIGINT, 0L);
        assertRow("byteMax / long2", EXPR0, BIGINT, (long) (Byte.MAX_VALUE / 2));
        assertDataError("byte1 / long0", "division by zero");

        assertRow("byte1 / float2", EXPR0, REAL, 0.5f);
        assertRow("byteMax / float2", EXPR0, REAL, Byte.MAX_VALUE / 2.0f);
        assertDataError("byte1 / float0", "division by zero");

        assertRow("byte1 / double2", EXPR0, DOUBLE, 0.5);
        assertRow("byteMax / double2", EXPR0, DOUBLE, Byte.MAX_VALUE / 2.0);
        assertDataError("byte1 / double0", "division by zero");

        assertRow("byte1 / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("byteMax / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Byte.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("byte1 / decimal0", "division by zero");

        assertRow("byte1 / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("byteMax / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Byte.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("byte1 / bigInteger0", "division by zero");

        assertParsingError("byte1 / string1", "Cannot apply [TINYINT, VARCHAR] to the '/' operator");
        assertParsingError("byte1 / char1", "Cannot apply [TINYINT, VARCHAR] to the '/' operator");
        assertParsingError("byte1 / dateCol", "Cannot apply [TINYINT, DATE] to the '/' operator");
        assertParsingError("byte1 / timeCol", "Cannot apply [TINYINT, TIME] to the '/' operator");
        assertParsingError("byte1 / dateTimeCol", "Cannot apply [TINYINT, TIMESTAMP] to the '/' operator");
        assertParsingError("byte1 / offsetDateTimeCol", "Cannot apply [TINYINT, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");
        assertParsingError("byte1 / object", "Cannot apply [TINYINT, OBJECT] to the '/' operator");
    }

    @Test
    public void testSmallint() {
        assertParsingError("short1 / booleanTrue", "Cannot apply [SMALLINT, BOOLEAN] to the '/' operator");

        assertRow("short1 / byte2", EXPR0, SMALLINT, (short) 0);
        assertRow("shortMax / byte2", EXPR0, SMALLINT, (short) (Short.MAX_VALUE / 2));
        assertDataError("short1 / byte0", "division by zero");

        assertRow("short1 / short2", EXPR0, SMALLINT, (short) 0);
        assertRow("shortMax / short2", EXPR0, SMALLINT, (short) (Short.MAX_VALUE / 2));
        assertDataError("short1 / short0", "division by zero");

        assertRow("short1 / int2", EXPR0, INTEGER, 0);
        assertRow("shortMax / int2", EXPR0, INTEGER, Short.MAX_VALUE / 2);
        assertDataError("short1 / int0", "division by zero");

        assertRow("short1 / long2", EXPR0, BIGINT, 0L);
        assertRow("shortMax / long2", EXPR0, BIGINT, (long) (Short.MAX_VALUE / 2));
        assertDataError("short1 / long0", "division by zero");

        assertRow("short1 / float2", EXPR0, REAL, 0.5f);
        assertRow("shortMax / float2", EXPR0, REAL, Short.MAX_VALUE / 2.0f);
        assertDataError("short1 / float0", "division by zero");

        assertRow("short1 / double2", EXPR0, DOUBLE, 0.5);
        assertRow("shortMax / double2", EXPR0, DOUBLE, Short.MAX_VALUE / 2.0);
        assertDataError("short1 / double0", "division by zero");

        assertRow("short1 / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("shortMax / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Short.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("short1 / decimal0", "division by zero");

        assertRow("short1 / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("shortMax / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Short.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("short1 / bigInteger0", "division by zero");

        assertParsingError("short1 / string1", "Cannot apply [SMALLINT, VARCHAR] to the '/' operator");
        assertParsingError("short1 / char1", "Cannot apply [SMALLINT, VARCHAR] to the '/' operator");
        assertParsingError("short1 / dateCol", "Cannot apply [SMALLINT, DATE] to the '/' operator");
        assertParsingError("short1 / timeCol", "Cannot apply [SMALLINT, TIME] to the '/' operator");
        assertParsingError("short1 / dateTimeCol", "Cannot apply [SMALLINT, TIMESTAMP] to the '/' operator");
        assertParsingError("short1 / offsetDateTimeCol", "Cannot apply [SMALLINT, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");
        assertParsingError("short1 / object", "Cannot apply [SMALLINT, OBJECT] to the '/' operator");
    }

    @Test
    public void testInteger() {
        assertParsingError("int1 / booleanTrue", "Cannot apply [INTEGER, BOOLEAN] to the '/' operator");

        assertRow("int1 / byte2", EXPR0, INTEGER, 0);
        assertRow("intMax / byte2", EXPR0, INTEGER, Integer.MAX_VALUE / 2);
        assertDataError("int1 / byte0", "division by zero");

        assertRow("int1 / short2", EXPR0, INTEGER, 0);
        assertRow("intMax / short2", EXPR0, INTEGER, Integer.MAX_VALUE / 2);
        assertDataError("int1 / short0", "division by zero");

        assertRow("int1 / int2", EXPR0, INTEGER, 0);
        assertRow("intMax / int2", EXPR0, INTEGER, Integer.MAX_VALUE / 2);
        assertDataError("int1 / int0", "division by zero");

        assertRow("int1 / long2", EXPR0, BIGINT, 0L);
        assertRow("intMax / long2", EXPR0, BIGINT, Integer.MAX_VALUE / 2L);
        assertDataError("int1 / long0", "division by zero");

        assertRow("int1 / float2", EXPR0, REAL, 0.5f);
        assertRow("intMax / float2", EXPR0, REAL, Integer.MAX_VALUE / 2.0f);
        assertDataError("int1 / float0", "division by zero");

        assertRow("int1 / double2", EXPR0, DOUBLE, 0.5);
        assertRow("intMax / double2", EXPR0, DOUBLE, Integer.MAX_VALUE / 2.0);
        assertDataError("int1 / double0", "division by zero");

        assertRow("int1 / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("intMax / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Integer.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("int1 / decimal0", "division by zero");

        assertRow("int1 / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("intMax / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Integer.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("int1 / bigInteger0", "division by zero");

        assertParsingError("int1 / string1", "Cannot apply [INTEGER, VARCHAR] to the '/' operator");
        assertParsingError("int1 / char1", "Cannot apply [INTEGER, VARCHAR] to the '/' operator");
        assertParsingError("int1 / dateCol", "Cannot apply [INTEGER, DATE] to the '/' operator");
        assertParsingError("int1 / timeCol", "Cannot apply [INTEGER, TIME] to the '/' operator");
        assertParsingError("int1 / dateTimeCol", "Cannot apply [INTEGER, TIMESTAMP] to the '/' operator");
        assertParsingError("int1 / offsetDateTimeCol", "Cannot apply [INTEGER, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");
        assertParsingError("int1 / object", "Cannot apply [INTEGER, OBJECT] to the '/' operator");
    }

    @Test
    public void testBigint() {
        assertParsingError("long1 / booleanTrue", "Cannot apply [BIGINT, BOOLEAN] to the '/' operator");

        assertRow("long1 / byte2", EXPR0, BIGINT, 0L);
        assertRow("longMax / byte2", EXPR0, BIGINT, Long.MAX_VALUE / 2L);
        assertDataError("long1 / byte0", "division by zero");

        assertRow("long1 / short2", EXPR0, BIGINT, 0L);
        assertRow("longMax / short2", EXPR0, BIGINT, Long.MAX_VALUE / 2L);
        assertDataError("long1 / short0", "division by zero");

        assertRow("long1 / int2", EXPR0, BIGINT, 0L);
        assertRow("longMax / int2", EXPR0, BIGINT, Long.MAX_VALUE / 2L);
        assertDataError("long1 / int0", "division by zero");

        assertRow("long1 / long2", EXPR0, BIGINT, 0L);
        assertRow("longMax / long2", EXPR0, BIGINT, Long.MAX_VALUE / 2L);
        assertDataError("long1 / long0", "division by zero");

        assertRow("long1 / float2", EXPR0, REAL, 0.5f);
        assertRow("longMax / float2", EXPR0, REAL, Long.MAX_VALUE / 2.0f);
        assertDataError("long1 / float0", "division by zero");

        assertRow("long1 / double2", EXPR0, DOUBLE, 0.5);
        assertRow("longMax / double2", EXPR0, DOUBLE, Long.MAX_VALUE / 2.0);
        assertDataError("long1 / double0", "division by zero");

        assertRow("long1 / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("longMax / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Long.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("long1 / decimal0", "division by zero");

        assertRow("long1 / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("longMax / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Long.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("long1 / bigInteger0", "division by zero");

        assertParsingError("long1 / string1", "Cannot apply [BIGINT, VARCHAR] to the '/' operator");
        assertParsingError("long1 / char1", "Cannot apply [BIGINT, VARCHAR] to the '/' operator");
        assertParsingError("long1 / dateCol", "Cannot apply [BIGINT, DATE] to the '/' operator");
        assertParsingError("long1 / timeCol", "Cannot apply [BIGINT, TIME] to the '/' operator");
        assertParsingError("long1 / dateTimeCol", "Cannot apply [BIGINT, TIMESTAMP] to the '/' operator");
        assertParsingError("long1 / offsetDateTimeCol", "Cannot apply [BIGINT, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("long1 / object", "Cannot apply [BIGINT, OBJECT] to the '/' operator");
    }

    @Test
    public void testReal() {
        assertParsingError("float1 / booleanTrue", "Cannot apply [REAL, BOOLEAN] to the '/' operator");

        assertRow("float1 / byte2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / byte2", EXPR0, REAL, Float.MAX_VALUE / 2);
        assertDataError("float1 / byte0", "division by zero");

        assertRow("float1 / short2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / short2", EXPR0, REAL, Float.MAX_VALUE / 2);
        assertDataError("float1 / short0", "division by zero");

        assertRow("float1 / int2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / int2", EXPR0, REAL, Float.MAX_VALUE / 2);
        assertDataError("float1 / int0", "division by zero");

        assertRow("float1 / long2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / long2", EXPR0, REAL, Float.MAX_VALUE / 2L);
        assertDataError("float1 / long0", "division by zero");

        assertRow("float1 / float2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / float2", EXPR0, REAL, Float.MAX_VALUE / 2.0f);
        assertDataError("float1 / float0", "division by zero");

        assertRow("float1 / double2", EXPR0, DOUBLE, 0.5);
        assertRow("floatMax / double2", EXPR0, DOUBLE, Float.MAX_VALUE / 2.0);
        assertDataError("float1 / double0", "division by zero");

        assertRow("float1 / decimal2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / decimal2", EXPR0, REAL, Float.MAX_VALUE / 2);
        assertDataError("float1 / decimal0", "division by zero");

        assertRow("float1 / bigInteger2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / bigInteger2", EXPR0, REAL, Float.MAX_VALUE / 2);
        assertDataError("float1 / bigInteger0", "division by zero");

        assertParsingError("float1 / string1", "Cannot apply [REAL, VARCHAR] to the '/' operator");
        assertParsingError("float1 / char1", "Cannot apply [REAL, VARCHAR] to the '/' operator");
        assertParsingError("float1 / dateCol", "Cannot apply [REAL, DATE] to the '/' operator");
        assertParsingError("float1 / timeCol", "Cannot apply [REAL, TIME] to the '/' operator");
        assertParsingError("float1 / dateTimeCol", "Cannot apply [REAL, TIMESTAMP] to the '/' operator");
        assertParsingError("float1 / offsetDateTimeCol", "Cannot apply [REAL, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("float1 / object", "Cannot apply [REAL, OBJECT] to the '/' operator");
    }

    @Test
    public void testDouble() {
        assertParsingError("double1 / booleanTrue", "Cannot apply [DOUBLE, BOOLEAN] to the '/' operator");

        assertRow("double1 / byte2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / byte2", EXPR0, DOUBLE, Double.MAX_VALUE / 2);
        assertDataError("double1 / byte0", "division by zero");

        assertRow("double1 / short2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / short2", EXPR0, DOUBLE, Double.MAX_VALUE / 2);
        assertDataError("double1 / short0", "division by zero");

        assertRow("double1 / int2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / int2", EXPR0, DOUBLE, Double.MAX_VALUE / 2);
        assertDataError("double1 / int0", "division by zero");

        assertRow("double1 / long2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / long2", EXPR0, DOUBLE, Double.MAX_VALUE / 2L);
        assertDataError("double1 / long0", "division by zero");

        assertRow("double1 / float2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / float2", EXPR0, DOUBLE, Double.MAX_VALUE / 2.0f);
        assertDataError("double1 / float0", "division by zero");

        assertRow("double1 / double2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / double2", EXPR0, DOUBLE, Double.MAX_VALUE / 2.0);
        assertDataError("double1 / double0", "division by zero");

        assertRow("double1 / decimal2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / decimal2", EXPR0, DOUBLE, Double.MAX_VALUE / 2);
        assertDataError("double1 / decimal0", "division by zero");

        assertRow("double1 / bigInteger2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / bigInteger2", EXPR0, DOUBLE, Double.MAX_VALUE / 2);
        assertDataError("double1 / bigInteger0", "division by zero");

        assertParsingError("double1 / string1", "Cannot apply [DOUBLE, VARCHAR] to the '/' operator");
        assertParsingError("double1 / char1", "Cannot apply [DOUBLE, VARCHAR] to the '/' operator");
        assertParsingError("double1 / dateCol", "Cannot apply [DOUBLE, DATE] to the '/' operator");
        assertParsingError("double1 / timeCol", "Cannot apply [DOUBLE, TIME] to the '/' operator");
        assertParsingError("double1 / dateTimeCol", "Cannot apply [DOUBLE, TIMESTAMP] to the '/' operator");
        assertParsingError("double1 / offsetDateTimeCol", "Cannot apply [DOUBLE, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("double1 / object", "Cannot apply [DOUBLE, OBJECT] to the '/' operator");
    }

    @Test
    public void testDecimal() {
        assertParsingError("decimal1 / booleanTrue", "Cannot apply [DECIMAL, BOOLEAN] to the '/' operator");

        assertRow("decimal1 / byte2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("decimalBig / byte2", EXPR0, DECIMAL,
                getRecord().decimalBig.divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / byte0", "division by zero");

        assertRow("decimal1 / short2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("decimalBig / short2", EXPR0, DECIMAL,
                getRecord().decimalBig.divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / short0", "division by zero");

        assertRow("decimal1 / int2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("decimalBig / int2", EXPR0, DECIMAL,
                getRecord().decimalBig.divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / int0", "division by zero");

        assertRow("decimal1 / long2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("decimalBig / long2", EXPR0, DECIMAL,
                getRecord().decimalBig.divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / long0", "division by zero");

        assertRow("decimal1 / float2", EXPR0, REAL, 0.5f);
        assertRow("decimalBig / float2", EXPR0, REAL, getRecord().decimalBig.floatValue() / 2.0f);
        assertDataError("decimal1 / float0", "division by zero");

        assertRow("decimal1 / double2", EXPR0, DOUBLE, 0.5);
        assertRow("decimalBig / double2", EXPR0, DOUBLE, getRecord().decimalBig.doubleValue() / 2.0);
        assertDataError("decimal1 / double0", "division by zero");

        assertRow("decimal1 / decimal2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("decimalBig / decimal2", EXPR0, DECIMAL,
                getRecord().decimalBig.divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / decimal0", "division by zero");

        assertRow("decimal1 / bigInteger2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("decimalBig / bigInteger2", EXPR0, DECIMAL,
                getRecord().decimalBig.divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / bigInteger0", "division by zero");

        assertParsingError("decimal1 / string1", "Cannot apply [DECIMAL, VARCHAR] to the '/' operator");
        assertParsingError("decimal1 / char1", "Cannot apply [DECIMAL, VARCHAR] to the '/' operator");
        assertParsingError("decimal1 / dateCol", "Cannot apply [DECIMAL, DATE] to the '/' operator");
        assertParsingError("decimal1 / timeCol", "Cannot apply [DECIMAL, TIME] to the '/' operator");
        assertParsingError("decimal1 / dateTimeCol", "Cannot apply [DECIMAL, TIMESTAMP] to the '/' operator");
        assertParsingError("decimal1 / offsetDateTimeCol", "Cannot apply [DECIMAL, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("decimal1 / object", "Cannot apply [DECIMAL, OBJECT] to the '/' operator");
    }

    @Test
    public void testVarchar() {
        assertParsingError("string1 / booleanTrue", "Cannot apply [VARCHAR, BOOLEAN] to the '/' operator");
        assertParsingError("string1 / byte1", "Cannot apply [VARCHAR, TINYINT] to the '/' operator");
        assertParsingError("string1 / short1", "Cannot apply [VARCHAR, SMALLINT] to the '/' operator");
        assertParsingError("string1 / int1", "Cannot apply [VARCHAR, INTEGER] to the '/' operator");
        assertParsingError("string1 / long1", "Cannot apply [VARCHAR, BIGINT] to the '/' operator");
        assertParsingError("string1 / float1", "Cannot apply [VARCHAR, REAL] to the '/' operator");
        assertParsingError("string1 / double1", "Cannot apply [VARCHAR, DOUBLE] to the '/' operator");
        assertParsingError("string1 / decimal1", "Cannot apply [VARCHAR, DECIMAL] to the '/' operator");
        assertParsingError("string1 / bigInteger1", "Cannot apply [VARCHAR, DECIMAL] to the '/' operator");
        assertParsingError("string1 / string1", "Cannot apply [VARCHAR, VARCHAR] to the '/' operator");
        assertParsingError("string1 / char1", "Cannot apply [VARCHAR, VARCHAR] to the '/' operator");
        assertParsingError("string1 / dateCol", "Cannot apply [VARCHAR, DATE] to the '/' operator");
        assertParsingError("string1 / timeCol", "Cannot apply [VARCHAR, TIME] to the '/' operator");
        assertParsingError("string1 / dateTimeCol", "Cannot apply [VARCHAR, TIMESTAMP] to the '/' operator");
        assertParsingError("string1 / offsetDateTimeCol", "Cannot apply [VARCHAR, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");
        assertParsingError("string1 / object", "Cannot apply [VARCHAR, OBJECT] to the '/' operator");
    }

    @Test
    public void testDate() {
        assertParsingError("dateCol / booleanTrue", "Cannot apply [DATE, BOOLEAN] to the '/' operator");

        assertParsingError("dateCol / byte1", "Cannot apply [DATE, TINYINT] to the '/' operator");
        assertParsingError("dateCol / short1", "Cannot apply [DATE, SMALLINT] to the '/' operator");
        assertParsingError("dateCol / int1", "Cannot apply [DATE, INTEGER] to the '/' operator");
        assertParsingError("dateCol / long1", "Cannot apply [DATE, BIGINT] to the '/' operator");

        assertParsingError("dateCol / float1", "Cannot apply [DATE, REAL] to the '/' operator");
        assertParsingError("dateCol / double1", "Cannot apply [DATE, DOUBLE] to the '/' operator");

        assertParsingError("dateCol / decimal1", "Cannot apply [DATE, DECIMAL] to the '/' operator");
        assertParsingError("dateCol / bigInteger1", "Cannot apply [DATE, DECIMAL] to the '/' operator");

        assertParsingError("dateCol / string1", "Cannot apply [DATE, VARCHAR] to the '/' operator");
        assertParsingError("dateCol / char1", "Cannot apply [DATE, VARCHAR] to the '/' operator");

        assertParsingError("dateCol / dateCol", "Cannot apply [DATE, DATE] to the '/' operator");
        assertParsingError("dateCol / timeCol", "Cannot apply [DATE, TIME] to the '/' operator");
        assertParsingError("dateCol / dateTimeCol", "Cannot apply [DATE, TIMESTAMP] to the '/' operator");
        assertParsingError("dateCol / offsetDateTimeCol", "Cannot apply [DATE, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("dateCol / object", "Cannot apply [DATE, OBJECT] to the '/' operator");
    }

    @Test
    public void testTime() {
        assertParsingError("timeCol / booleanTrue", "Cannot apply [TIME, BOOLEAN] to the '/' operator");

        assertParsingError("timeCol / byte1", "Cannot apply [TIME, TINYINT] to the '/' operator");
        assertParsingError("timeCol / short1", "Cannot apply [TIME, SMALLINT] to the '/' operator");
        assertParsingError("timeCol / int1", "Cannot apply [TIME, INTEGER] to the '/' operator");
        assertParsingError("timeCol / long1", "Cannot apply [TIME, BIGINT] to the '/' operator");

        assertParsingError("timeCol / float1", "Cannot apply [TIME, REAL] to the '/' operator");
        assertParsingError("timeCol / double1", "Cannot apply [TIME, DOUBLE] to the '/' operator");

        assertParsingError("timeCol / decimal1", "Cannot apply [TIME, DECIMAL] to the '/' operator");
        assertParsingError("timeCol / bigInteger1", "Cannot apply [TIME, DECIMAL] to the '/' operator");

        assertParsingError("timeCol / string1", "Cannot apply [TIME, VARCHAR] to the '/' operator");
        assertParsingError("timeCol / char1", "Cannot apply [TIME, VARCHAR] to the '/' operator");

        assertParsingError("timeCol / dateCol", "Cannot apply [TIME, DATE] to the '/' operator");
        assertParsingError("timeCol / timeCol", "Cannot apply [TIME, TIME] to the '/' operator");
        assertParsingError("timeCol / dateTimeCol", "Cannot apply [TIME, TIMESTAMP] to the '/' operator");
        assertParsingError("timeCol / offsetDateTimeCol", "Cannot apply [TIME, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("timeCol / object", "Cannot apply [TIME, OBJECT] to the '/' operator");
    }

    @Test
    public void testTimestamp() {
        assertParsingError("dateTimeCol / booleanTrue", "Cannot apply [TIMESTAMP, BOOLEAN] to the '/' operator");

        assertParsingError("dateTimeCol / byte1", "Cannot apply [TIMESTAMP, TINYINT] to the '/' operator");
        assertParsingError("dateTimeCol / short1", "Cannot apply [TIMESTAMP, SMALLINT] to the '/' operator");
        assertParsingError("dateTimeCol / int1", "Cannot apply [TIMESTAMP, INTEGER] to the '/' operator");
        assertParsingError("dateTimeCol / long1", "Cannot apply [TIMESTAMP, BIGINT] to the '/' operator");

        assertParsingError("dateTimeCol / float1", "Cannot apply [TIMESTAMP, REAL] to the '/' operator");
        assertParsingError("dateTimeCol / double1", "Cannot apply [TIMESTAMP, DOUBLE] to the '/' operator");

        assertParsingError("dateTimeCol / decimal1", "Cannot apply [TIMESTAMP, DECIMAL] to the '/' operator");
        assertParsingError("dateTimeCol / bigInteger1", "Cannot apply [TIMESTAMP, DECIMAL] to the '/' operator");

        assertParsingError("dateTimeCol / string1", "Cannot apply [TIMESTAMP, VARCHAR] to the '/' operator");
        assertParsingError("dateTimeCol / char1", "Cannot apply [TIMESTAMP, VARCHAR] to the '/' operator");

        assertParsingError("dateTimeCol / dateCol", "Cannot apply [TIMESTAMP, DATE] to the '/' operator");
        assertParsingError("dateTimeCol / timeCol", "Cannot apply [TIMESTAMP, TIME] to the '/' operator");
        assertParsingError("dateTimeCol / dateTimeCol", "Cannot apply [TIMESTAMP, TIMESTAMP] to the '/' operator");
        assertParsingError("dateTimeCol / offsetDateTimeCol", "Cannot apply [TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("dateTimeCol / object", "Cannot apply [TIMESTAMP, OBJECT] to the '/' operator");
    }

    @Test
    public void testTimestampWithTimeZone() {
        assertParsingError("offsetDateTimeCol / booleanTrue", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, BOOLEAN] to the '/' operator");

        assertParsingError("offsetDateTimeCol / byte1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, TINYINT] to the '/' operator");
        assertParsingError("offsetDateTimeCol / short1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, SMALLINT] to the '/' operator");
        assertParsingError("offsetDateTimeCol / int1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, INTEGER] to the '/' operator");
        assertParsingError("offsetDateTimeCol / long1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, BIGINT] to the '/' operator");

        assertParsingError("offsetDateTimeCol / float1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, REAL] to the '/' operator");
        assertParsingError("offsetDateTimeCol / double1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, DOUBLE] to the '/' operator");

        assertParsingError("offsetDateTimeCol / decimal1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, DECIMAL] to the '/' operator");
        assertParsingError("offsetDateTimeCol / bigInteger1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, DECIMAL] to the '/' operator");

        assertParsingError("offsetDateTimeCol / string1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, VARCHAR] to the '/' operator");
        assertParsingError("offsetDateTimeCol / char1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, VARCHAR] to the '/' operator");

        assertParsingError("offsetDateTimeCol / dateCol", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, DATE] to the '/' operator");
        assertParsingError("offsetDateTimeCol / timeCol", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, TIME] to the '/' operator");
        assertParsingError("offsetDateTimeCol / dateTimeCol", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP] to the '/' operator");
        assertParsingError("offsetDateTimeCol / offsetDateTimeCol", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("offsetDateTimeCol / object", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, OBJECT] to the '/' operator");
    }

    @Test
    public void testObject() {
        assertParsingError("object / booleanTrue", "Cannot apply [OBJECT, BOOLEAN] to the '/' operator");

        assertParsingError("object / byte1", "Cannot apply [OBJECT, TINYINT] to the '/' operator");
        assertParsingError("object / short1", "Cannot apply [OBJECT, SMALLINT] to the '/' operator");
        assertParsingError("object / int1", "Cannot apply [OBJECT, INTEGER] to the '/' operator");
        assertParsingError("object / long1", "Cannot apply [OBJECT, BIGINT] to the '/' operator");

        assertParsingError("object / float1", "Cannot apply [OBJECT, REAL] to the '/' operator");
        assertParsingError("object / double1", "Cannot apply [OBJECT, DOUBLE] to the '/' operator");

        assertParsingError("object / decimal1", "Cannot apply [OBJECT, DECIMAL] to the '/' operator");
        assertParsingError("object / bigInteger1", "Cannot apply [OBJECT, DECIMAL] to the '/' operator");

        assertParsingError("object / string1", "Cannot apply [OBJECT, VARCHAR] to the '/' operator");
        assertParsingError("object / char1", "Cannot apply [OBJECT, VARCHAR] to the '/' operator");

        assertParsingError("object / dateCol", "Cannot apply [OBJECT, DATE] to the '/' operator");
        assertParsingError("object / timeCol", "Cannot apply [OBJECT, TIME] to the '/' operator");
        assertParsingError("object / dateTimeCol", "Cannot apply [OBJECT, TIMESTAMP] to the '/' operator");
        assertParsingError("object / offsetDateTimeCol", "Cannot apply [OBJECT, TIMESTAMP_WITH_TIME_ZONE] to the '/' operator");

        assertParsingError("object / object", "Cannot apply [OBJECT, OBJECT] to the '/' operator");
    }

}
