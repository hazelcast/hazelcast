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
public class DivideIntegrationTest extends ExpressionIntegrationTestBase {

    @Test
    public void testBoolean() {
        assertParsingError("booleanTrue / booleanTrue", "Cannot apply '/' to arguments of type '<BOOLEAN> / <BOOLEAN>'");

        assertParsingError("booleanTrue / byte1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <TINYINT>'");
        assertParsingError("booleanTrue / short1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <SMALLINT>'");
        assertParsingError("booleanTrue / int1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <INTEGER>'");
        assertParsingError("booleanTrue / long1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <BIGINT>'");

        assertParsingError("booleanTrue / float1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <REAL>'");
        assertParsingError("booleanTrue / double1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <DOUBLE>'");

        assertParsingError("booleanTrue / decimal1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <DECIMAL(38, 38)>'");
        assertParsingError("booleanTrue / bigInteger1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <DECIMAL(38, 38)>'");

        assertParsingError("booleanTrue / string1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <BOOLEAN>'");
        assertParsingError("booleanTrue / char1", "Cannot apply '/' to arguments of type '<BOOLEAN> / <BOOLEAN>'");

        assertParsingError("booleanTrue / object", "Cannot apply '/' to arguments of type '<BOOLEAN> / <OBJECT>'");
    }

    @Test
    public void testTinyint() {
        assertParsingError("byte1 / booleanTrue", "Cannot apply '/' to arguments of type '<TINYINT> / <BOOLEAN>'");

        assertRow("byte1 / byte2", EXPR0, SMALLINT, (short) 0);
        assertRow("byteMax / byte2", EXPR0, SMALLINT, (short) (Byte.MAX_VALUE / 2));
        assertDataError("byte1 / byte0", "division by zero");

        assertRow("byte1 / short2", EXPR0, SMALLINT, (short) 0);
        assertRow("byteMax / short2", EXPR0, SMALLINT, (short) (Byte.MAX_VALUE / 2));
        assertDataError("byte1 / short0", "division by zero");

        assertRow("byte1 / int2", EXPR0, SMALLINT, (short) 0);
        assertRow("byteMax / int2", EXPR0, SMALLINT, (short) (Byte.MAX_VALUE / 2));
        assertDataError("byte1 / int0", "division by zero");

        assertRow("byte1 / long2", EXPR0, SMALLINT, (short) 0);
        assertRow("byteMax / long2", EXPR0, SMALLINT, (short) (Byte.MAX_VALUE / 2));
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

        assertRow("byte1 / string2", EXPR0, SMALLINT, (short) 0);
        assertRow("byteMax / string2", EXPR0, SMALLINT, (short) (Byte.MAX_VALUE / 2));
        assertDataError("byte1 / string0", "division by zero");
        assertDataError("byte1 / stringBig", "Cannot convert VARCHAR to BIGINT");
        assertDataError("byte1 / stringFoo", "Cannot convert VARCHAR to BIGINT");

        assertRow("byte1 / char2", EXPR0, SMALLINT, (short) 0);
        assertRow("byteMax / char2", EXPR0, SMALLINT, (short) (Byte.MAX_VALUE / 2));
        assertDataError("byte1 / char0", "division by zero");
        assertDataError("byte1 / charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("byte1 / object", "Cannot apply '/' to arguments of type '<TINYINT> / <OBJECT>'");
    }

    @Test
    public void testSmallint() {
        assertParsingError("short1 / booleanTrue", "Cannot apply '/' to arguments of type '<SMALLINT> / <BOOLEAN>'");

        assertRow("short1 / byte2", EXPR0, INTEGER, 0);
        assertRow("shortMax / byte2", EXPR0, INTEGER, Short.MAX_VALUE / 2);
        assertDataError("short1 / byte0", "division by zero");

        assertRow("short1 / short2", EXPR0, INTEGER, 0);
        assertRow("shortMax / short2", EXPR0, INTEGER, Short.MAX_VALUE / 2);
        assertDataError("short1 / short0", "division by zero");

        assertRow("short1 / int2", EXPR0, INTEGER, 0);
        assertRow("shortMax / int2", EXPR0, INTEGER, Short.MAX_VALUE / 2);
        assertDataError("short1 / int0", "division by zero");

        assertRow("short1 / long2", EXPR0, INTEGER, 0);
        assertRow("shortMax / long2", EXPR0, INTEGER, Short.MAX_VALUE / 2);
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

        assertRow("short1 / string2", EXPR0, INTEGER, 0);
        assertRow("shortMax / string2", EXPR0, INTEGER, Short.MAX_VALUE / 2);
        assertDataError("short1 / string0", "division by zero");
        assertDataError("short1 / stringBig", "Cannot convert VARCHAR to BIGINT");
        assertDataError("short1 / stringFoo", "Cannot convert VARCHAR to BIGINT");

        assertRow("short1 / char2", EXPR0, INTEGER, 0);
        assertRow("shortMax / char2", EXPR0, INTEGER, Short.MAX_VALUE / 2);
        assertDataError("short1 / char0", "division by zero");
        assertDataError("short1 / charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("short1 / object", "Cannot apply '/' to arguments of type '<SMALLINT> / <OBJECT>'");
    }

    @Test
    public void testInteger() {
        assertParsingError("int1 / booleanTrue", "Cannot apply '/' to arguments of type '<INTEGER> / <BOOLEAN>'");

        assertRow("int1 / byte2", EXPR0, BIGINT, 0L);
        assertRow("intMax / byte2", EXPR0, BIGINT, Integer.MAX_VALUE / 2L);
        assertDataError("int1 / byte0", "division by zero");

        assertRow("int1 / short2", EXPR0, BIGINT, 0L);
        assertRow("intMax / short2", EXPR0, BIGINT, Integer.MAX_VALUE / 2L);
        assertDataError("int1 / short0", "division by zero");

        assertRow("int1 / int2", EXPR0, BIGINT, 0L);
        assertRow("intMax / int2", EXPR0, BIGINT, Integer.MAX_VALUE / 2L);
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

        assertRow("int1 / string2", EXPR0, BIGINT, 0L);
        assertRow("intMax / string2", EXPR0, BIGINT, Integer.MAX_VALUE / 2L);
        assertDataError("int1 / string0", "division by zero");
        assertDataError("int1 / stringBig", "Cannot convert VARCHAR to BIGINT");
        assertDataError("int1 / stringFoo", "Cannot convert VARCHAR to BIGINT");

        assertRow("int1 / char2", EXPR0, BIGINT, 0L);
        assertRow("intMax / char2", EXPR0, BIGINT, Integer.MAX_VALUE / 2L);
        assertDataError("int1 / char0", "division by zero");
        assertDataError("int1 / charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("int1 / object", "Cannot apply '/' to arguments of type '<INTEGER> / <OBJECT>'");
    }

    @Test
    public void testBigint() {
        assertParsingError("long1 / booleanTrue", "Cannot apply '/' to arguments of type '<BIGINT> / <BOOLEAN>'");

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

        assertRow("long1 / string2", EXPR0, BIGINT, 0L);
        assertRow("longMax / string2", EXPR0, BIGINT, Long.MAX_VALUE / 2L);
        assertDataError("long1 / string0", "division by zero");
        assertDataError("long1 / stringBig", "Cannot convert VARCHAR to BIGINT");
        assertDataError("long1 / stringFoo", "Cannot convert VARCHAR to BIGINT");

        assertRow("long1 / char2", EXPR0, BIGINT, 0L);
        assertRow("longMax / char2", EXPR0, BIGINT, Long.MAX_VALUE / 2L);
        assertDataError("long1 / char0", "division by zero");
        assertDataError("long1 / charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("long1 / object", "Cannot apply '/' to arguments of type '<BIGINT> / <OBJECT>'");
    }

    @Test
    public void testReal() {
        assertParsingError("float1 / booleanTrue", "Cannot apply '/' to arguments of type '<REAL> / <BOOLEAN>'");

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

        assertRow("float1 / string2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / string2", EXPR0, REAL, Float.MAX_VALUE / 2);
        assertDataError("float1 / string0", "division by zero");
        assertRow("float1 / stringBig", EXPR0, REAL, 1.0f / Float.parseFloat(getRecord().stringBig));
        assertDataError("float1 / stringFoo", "Cannot convert VARCHAR to REAL");

        assertRow("float1 / char2", EXPR0, REAL, 0.5f);
        assertRow("floatMax / char2", EXPR0, REAL, Float.MAX_VALUE / 2);
        assertDataError("float1 / char0", "division by zero");
        assertDataError("float1 / charF", "Cannot convert VARCHAR to REAL");

        assertParsingError("float1 / object", "Cannot apply '/' to arguments of type '<REAL> / <OBJECT>'");
    }

    @Test
    public void testDouble() {
        assertParsingError("double1 / booleanTrue", "Cannot apply '/' to arguments of type '<DOUBLE> / <BOOLEAN>'");

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

        assertRow("double1 / string2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / string2", EXPR0, DOUBLE, Double.MAX_VALUE / 2);
        assertDataError("double1 / string0", "division by zero");
        assertRow("double1 / stringBig", EXPR0, DOUBLE, 1.0 / Double.parseDouble(getRecord().stringBig));
        assertDataError("double1 / stringFoo", "Cannot convert VARCHAR to DOUBLE");

        assertRow("double1 / char2", EXPR0, DOUBLE, 0.5);
        assertRow("doubleMax / char2", EXPR0, DOUBLE, Double.MAX_VALUE / 2);
        assertDataError("double1 / char0", "division by zero");
        assertDataError("double1 / charF", "Cannot convert VARCHAR to DOUBLE");

        assertParsingError("double1 / object", "Cannot apply '/' to arguments of type '<DOUBLE> / <OBJECT>'");
    }

    @Test
    public void testDecimal() {
        assertParsingError("decimal1 / booleanTrue", "Cannot apply '/' to arguments of type '<DECIMAL(38, 38)> / <BOOLEAN>'");

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

        assertRow("decimal1 / string2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("decimalBig / string2", EXPR0, DECIMAL,
                getRecord().decimalBig.divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / string0", "division by zero");
        assertRow("decimal1 / stringBig", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).divide(new BigDecimal(getRecord().stringBig), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / stringFoo", "Cannot convert VARCHAR to DECIMAL");

        assertRow("decimal1 / char2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("decimalBig / char2", EXPR0, DECIMAL,
                getRecord().decimalBig.divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("decimal1 / char0", "division by zero");
        assertDataError("decimal1 / charF", "Cannot convert VARCHAR to DECIMAL");

        assertParsingError("decimal1 / object", "Cannot apply '/' to arguments of type '<DECIMAL(38, 38)> / <OBJECT>'");
    }

    @Test
    public void testVarchar() {
        assertParsingError("string1 / booleanTrue", "Cannot apply '/' to arguments of type '<BOOLEAN> / <BOOLEAN>'");

        assertRow("string1 / byte2", EXPR0, BIGINT, 0L);
        assertRow("stringLongMax / byte2", EXPR0, BIGINT, Long.MAX_VALUE / 2);
        assertDataError("string1 / byte0", "division by zero");

        assertRow("string1 / short2", EXPR0, BIGINT, 0L);
        assertRow("stringLongMax / short2", EXPR0, BIGINT, Long.MAX_VALUE / 2);
        assertDataError("string1 / short0", "division by zero");

        assertRow("string1 / int2", EXPR0, BIGINT, 0L);
        assertRow("stringLongMax / int2", EXPR0, BIGINT, Long.MAX_VALUE / 2);
        assertDataError("string1 / int0", "division by zero");

        assertRow("string1 / long2", EXPR0, BIGINT, 0L);
        assertRow("stringLongMax / long2", EXPR0, BIGINT, Long.MAX_VALUE / 2);
        assertDataError("string1 / long0", "division by zero");

        assertRow("string1 / float2", EXPR0, REAL, 0.5f);
        assertRow("stringLongMax / float2", EXPR0, REAL, Long.MAX_VALUE / 2.0f);
        assertDataError("string1 / float0", "division by zero");

        assertRow("string1 / double2", EXPR0, DOUBLE, 0.5);
        assertRow("stringLongMax / double2", EXPR0, DOUBLE, Long.MAX_VALUE / 2.0);
        assertDataError("string1 / double0", "division by zero");

        assertRow("string1 / decimal2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("stringLongMax / decimal2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Long.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("string1 / decimal0", "division by zero");

        assertRow("string1 / bigInteger2", EXPR0, DECIMAL, BigDecimal.valueOf(0.5));
        assertRow("stringLongMax / bigInteger2", EXPR0, DECIMAL,
                BigDecimal.valueOf(Long.MAX_VALUE).divide(BigDecimal.valueOf(2), ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertDataError("string1 / bigInteger0", "division by zero");

        assertRow("string1 / string1", EXPR0, DOUBLE, 1.0);
        assertRow("string1 / string2", EXPR0, DOUBLE, 0.5);
        assertRow("string1 / stringBig", EXPR0, DOUBLE, 1.0 / Double.parseDouble(getRecord().stringBig));
        assertDataError("string1 / stringFoo", "Cannot convert VARCHAR to DOUBLE");
        assertRow("string1 / char1", EXPR0, DOUBLE, 1.0);
        assertDataError("string1 / charF", "Cannot convert VARCHAR to DOUBLE");

        assertDataError("stringBig / int1", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("string1 / object", "Cannot apply '/' to arguments of type '<VARCHAR> / <OBJECT>'");
    }

    @Test
    public void testObject() {
        assertParsingError("object / booleanTrue", "Cannot apply '/' to arguments of type '<OBJECT> / <BOOLEAN>'");

        assertParsingError("object / byte1", "Cannot apply '/' to arguments of type '<OBJECT> / <TINYINT>'");
        assertParsingError("object / short1", "Cannot apply '/' to arguments of type '<OBJECT> / <SMALLINT>'");
        assertParsingError("object / int1", "Cannot apply '/' to arguments of type '<OBJECT> / <INTEGER>'");
        assertParsingError("object / long1", "Cannot apply '/' to arguments of type '<OBJECT> / <BIGINT>'");

        assertParsingError("object / float1", "Cannot apply '/' to arguments of type '<OBJECT> / <REAL>'");
        assertParsingError("object / double1", "Cannot apply '/' to arguments of type '<OBJECT> / <DOUBLE>'");

        assertParsingError("object / decimal1", "Cannot apply '/' to arguments of type '<OBJECT> / <DECIMAL(38, 38)>'");
        assertParsingError("object / bigInteger1", "Cannot apply '/' to arguments of type '<OBJECT> / <DECIMAL(38, 38)>'");

        assertParsingError("object / string1", "Cannot apply '/' to arguments of type '<OBJECT> / <VARCHAR>'");
        assertParsingError("object / char1", "Cannot apply '/' to arguments of type '<OBJECT> / <VARCHAR>'");

        assertParsingError("object / object", "Cannot apply '/' to arguments of type '<OBJECT> / <OBJECT>'");
    }

}
