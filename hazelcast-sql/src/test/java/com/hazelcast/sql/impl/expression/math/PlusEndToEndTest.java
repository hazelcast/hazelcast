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

import com.hazelcast.sql.impl.expression.ExpressionEndToEndTestBase;
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
import static com.hazelcast.sql.SqlColumnType.INT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PlusEndToEndTest extends ExpressionEndToEndTestBase {

    @Test
    public void testBoolean() {
        assertParsingError("booleanTrue + booleanTrue", "Cannot apply '+' to arguments of type '<BOOLEAN> + <BOOLEAN>'");

        assertParsingError("booleanTrue + byte1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <TINYINT>'");
        assertParsingError("booleanTrue + short1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <SMALLINT>'");
        assertParsingError("booleanTrue + int1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <INTEGER>'");
        assertParsingError("booleanTrue + long1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <BIGINT>'");

        assertParsingError("booleanTrue + float1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <REAL>'");
        assertParsingError("booleanTrue + double1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <DOUBLE>'");

        assertParsingError("booleanTrue + decimal1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <DECIMAL(38, 38)>'");
        assertParsingError("booleanTrue + bigInteger1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <DECIMAL(38, 38)>'");

        assertParsingError("booleanTrue + string1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <BOOLEAN>'");
        assertParsingError("booleanTrue + char1", "Cannot apply '+' to arguments of type '<BOOLEAN> + <BOOLEAN>'");

        assertParsingError("booleanTrue + object", "Cannot apply '+' to arguments of type '<BOOLEAN> + <ANY>'");
    }

    @Test
    public void testTinyint() {
        assertParsingError("byte1 + booleanTrue", "Cannot apply '+' to arguments of type '<TINYINT> + <BOOLEAN>'");

        assertRow("byte1 + byte1", EXPR0, SMALLINT, (short) 2);
        assertRow("byte1 + byteMax", EXPR0, SMALLINT, (short) (1 + Byte.MAX_VALUE));
        assertRow("byte1 + short1", EXPR0, INT, 2);
        assertRow("byte1 + shortMax", EXPR0, INT, 1 + Short.MAX_VALUE);
        assertRow("byte1 + int1", EXPR0, BIGINT, 2L);
        assertRow("byte1 + intMax", EXPR0, BIGINT, 1L + Integer.MAX_VALUE);
        assertRow("byte1 + long1", EXPR0, BIGINT, 2L);
        assertDataError("byte1 + longMax", "BIGINT overflow");

        assertRow("byte1 + float1", EXPR0, REAL, 2.0f);
        assertRow("byte1 + floatMax", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("byte1 + double1", EXPR0, DOUBLE, 2.0);
        assertRow("byte1 + doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("byte1 + decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("byte1 + decimalBig", EXPR0, DECIMAL, getRecord().decimalBig.add(BigDecimal.valueOf(1)));
        assertRow("byte1 + bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("byte1 + bigIntegerBig", EXPR0, DECIMAL, getRecord().decimalBig.add(BigDecimal.valueOf(1)));

        assertRow("byte1 + string1", EXPR0, BIGINT, 2L);
        assertDataError("byte1 + stringBig", "Cannot convert VARCHAR to BIGINT: 92233720368547758070");
        assertDataError("byte1 + stringFoo", "Cannot convert VARCHAR to BIGINT: foo");
        assertRow("byte1 + char1", EXPR0, BIGINT, 2L);
        assertDataError("byte1 + charF", "Cannot convert VARCHAR to BIGINT: f");

        assertParsingError("byte1 + object", "Cannot apply '+' to arguments of type '<TINYINT> + <ANY>'");
    }

    @Test
    public void testSmallint() {
        assertParsingError("short1 + booleanTrue", "Cannot apply '+' to arguments of type '<SMALLINT> + <BOOLEAN>'");

        assertRow("short1 + byte1", EXPR0, INT, 2);
        assertRow("short1 + byteMax", EXPR0, INT, 1 + Byte.MAX_VALUE);
        assertRow("short1 + short1", EXPR0, INT, 2);
        assertRow("short1 + shortMax", EXPR0, INT, 1 + Short.MAX_VALUE);
        assertRow("short1 + int1", EXPR0, BIGINT, 2L);
        assertRow("short1 + intMax", EXPR0, BIGINT, 1L + Integer.MAX_VALUE);
        assertRow("short1 + long1", EXPR0, BIGINT, 2L);
        assertDataError("short1 + longMax", "BIGINT overflow");

        assertRow("short1 + float1", EXPR0, REAL, 2.0f);
        assertRow("short1 + floatMax", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("short1 + double1", EXPR0, DOUBLE, 2.0);
        assertRow("short1 + doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("short1 + decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("short1 + decimalBig", EXPR0, DECIMAL, getRecord().decimalBig.add(BigDecimal.valueOf(1)));
        assertRow("short1 + bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("short1 + bigIntegerBig", EXPR0, DECIMAL, getRecord().decimalBig.add(BigDecimal.valueOf(1)));

        assertRow("short1 + string1", EXPR0, BIGINT, 2L);
        assertDataError("short1 + stringBig", "Cannot convert VARCHAR to BIGINT: 92233720368547758070");
        assertDataError("short1 + stringFoo", "Cannot convert VARCHAR to BIGINT: foo");
        assertRow("short1 + char1", EXPR0, BIGINT, 2L);
        assertDataError("short1 + charF", "Cannot convert VARCHAR to BIGINT: f");

        assertParsingError("short1 + object", "Cannot apply '+' to arguments of type '<SMALLINT> + <ANY>'");
    }

    @Test
    public void testInteger() {
        assertParsingError("int1 + booleanTrue", "Cannot apply '+' to arguments of type '<INTEGER> + <BOOLEAN>'");

        assertRow("int1 + byte1", EXPR0, BIGINT, 2L);
        assertRow("int1 + byteMax", EXPR0, BIGINT, 1L + Byte.MAX_VALUE);
        assertRow("int1 + short1", EXPR0, BIGINT, 2L);
        assertRow("int1 + shortMax", EXPR0, BIGINT, 1L + Short.MAX_VALUE);
        assertRow("int1 + int1", EXPR0, BIGINT, 2L);
        assertRow("int1 + intMax", EXPR0, BIGINT, 1L + Integer.MAX_VALUE);
        assertRow("int1 + long1", EXPR0, BIGINT, 2L);
        assertDataError("int1 + longMax", "BIGINT overflow");

        assertRow("int1 + float1", EXPR0, REAL, 2.0f);
        assertRow("int1 + floatMax", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("int1 + double1", EXPR0, DOUBLE, 2.0);
        assertRow("int1 + doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("int1 + decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("int1 + decimalBig", EXPR0, DECIMAL, getRecord().decimalBig.add(BigDecimal.valueOf(1)));
        assertRow("int1 + bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("int1 + bigIntegerBig", EXPR0, DECIMAL, getRecord().decimalBig.add(BigDecimal.valueOf(1)));

        assertRow("int1 + string1", EXPR0, BIGINT, 2L);
        assertDataError("int1 + stringBig", "Cannot convert VARCHAR to BIGINT: 92233720368547758070");
        assertDataError("int1 + stringFoo", "Cannot convert VARCHAR to BIGINT: foo");
        assertRow("int1 + char1", EXPR0, BIGINT, 2L);
        assertDataError("int1 + charF", "Cannot convert VARCHAR to BIGINT: f");

        assertParsingError("int1 + object", "Cannot apply '+' to arguments of type '<INTEGER> + <ANY>'");
    }

    @Test
    public void testBigint() {
        assertParsingError("long1 + booleanTrue", "Cannot apply '+' to arguments of type '<BIGINT> + <BOOLEAN>'");

        assertRow("long1 + byte1", EXPR0, BIGINT, 2L);
        assertRow("long1 + byteMax", EXPR0, BIGINT, 1L + Byte.MAX_VALUE);
        assertRow("long1 + short1", EXPR0, BIGINT, 2L);
        assertRow("long1 + shortMax", EXPR0, BIGINT, 1L + Short.MAX_VALUE);
        assertRow("long1 + int1", EXPR0, BIGINT, 2L);
        assertRow("long1 + intMax", EXPR0, BIGINT, 1L + Integer.MAX_VALUE);
        assertRow("long1 + long1", EXPR0, BIGINT, 2L);
        assertDataError("long1 + longMax", "BIGINT overflow");

        assertRow("long1 + float1", EXPR0, REAL, 2.0f);
        assertRow("long1 + floatMax", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("long1 + double1", EXPR0, DOUBLE, 2.0);
        assertRow("long1 + doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("long1 + decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("long1 + decimalBig", EXPR0, DECIMAL, getRecord().decimalBig.add(BigDecimal.valueOf(1)));
        assertRow("long1 + bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("long1 + bigIntegerBig", EXPR0, DECIMAL, getRecord().decimalBig.add(BigDecimal.valueOf(1)));

        assertRow("long1 + string1", EXPR0, BIGINT, 2L);
        assertDataError("long1 + stringBig", "Cannot convert VARCHAR to BIGINT: 92233720368547758070");
        assertDataError("long1 + stringFoo", "Cannot convert VARCHAR to BIGINT: foo");
        assertRow("long1 + char1", EXPR0, BIGINT, 2L);
        assertDataError("long1 + charF", "Cannot convert VARCHAR to BIGINT: f");

        assertParsingError("long1 + object", "Cannot apply '+' to arguments of type '<BIGINT> + <ANY>'");
    }

    @Test
    public void testReal() {
        assertParsingError("float1 + booleanTrue", "Cannot apply '+' to arguments of type '<REAL> + <BOOLEAN>'");

        assertRow("float1 + byte1", EXPR0, REAL, 2.0f);
        assertRow("float1 + byteMax", EXPR0, REAL, 1.0f + Byte.MAX_VALUE);
        assertRow("float1 + short1", EXPR0, REAL, 2.0f);
        assertRow("float1 + shortMax", EXPR0, REAL, 1.0f + Short.MAX_VALUE);
        assertRow("float1 + int1", EXPR0, REAL, 2.0f);
        assertRow("float1 + intMax", EXPR0, REAL, 1.0f + Integer.MAX_VALUE);
        assertRow("float1 + long1", EXPR0, REAL, 2.0f);
        assertRow("float1 + longMax", EXPR0, REAL, 1.0f + Long.MAX_VALUE);

        assertRow("float1 + float1", EXPR0, REAL, 2.0f);
        assertRow("float1 + floatMax", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("float1 + double1", EXPR0, DOUBLE, 2.0);
        assertRow("float1 + doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("float1 + decimal1", EXPR0, REAL, 2.0f);
        assertRow("float1 + decimalBig", EXPR0, REAL, 1.0f + getRecord().decimalBig.floatValue());
        assertRow("float1 + bigInteger1", EXPR0, REAL, 2.0f);
        assertRow("float1 + bigIntegerBig", EXPR0, REAL, 1.0f + getRecord().bigIntegerBig.floatValue());

        assertRow("float1 + string1", EXPR0, REAL, 2.0f);
        assertRow("float1 + stringBig", EXPR0, REAL, 1.0f + Float.parseFloat(getRecord().stringBig));
        assertDataError("float1 + stringFoo", "Cannot convert VARCHAR to REAL: foo");
        assertRow("float1 + char1", EXPR0, REAL, 2.0f);
        assertDataError("float1 + charF", "Cannot convert VARCHAR to REAL: f");

        assertParsingError("float1 + object", "Cannot apply '+' to arguments of type '<REAL> + <ANY>'");
    }

    @Test
    public void testDouble() {
        assertParsingError("double1 + booleanTrue", "Cannot apply '+' to arguments of type '<DOUBLE> + <BOOLEAN>'");

        assertRow("double1 + byte1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + byteMax", EXPR0, DOUBLE, 1.0 + Byte.MAX_VALUE);
        assertRow("double1 + short1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + shortMax", EXPR0, DOUBLE, 1.0 + Short.MAX_VALUE);
        assertRow("double1 + int1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + intMax", EXPR0, DOUBLE, 1.0 + Integer.MAX_VALUE);
        assertRow("double1 + long1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + longMax", EXPR0, DOUBLE, 1.0 + Long.MAX_VALUE);

        assertRow("double1 + float1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + floatMax", EXPR0, DOUBLE, (double) Float.MAX_VALUE);
        assertRow("double1 + double1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("double1 + decimal1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + decimalBig", EXPR0, DOUBLE, 1.0 + getRecord().decimalBig.floatValue());
        assertRow("double1 + bigInteger1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + bigIntegerBig", EXPR0, DOUBLE, 1.0 + getRecord().bigIntegerBig.floatValue());

        assertRow("double1 + string1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + stringBig", EXPR0, DOUBLE, 1.0 + Float.parseFloat(getRecord().stringBig));
        assertDataError("double1 + stringFoo", "Cannot convert VARCHAR to DOUBLE: foo");
        assertRow("double1 + char1", EXPR0, DOUBLE, 2.0);
        assertDataError("double1 + charF", "Cannot convert VARCHAR to DOUBLE: f");

        assertParsingError("double1 + object", "Cannot apply '+' to arguments of type '<DOUBLE> + <ANY>'");
    }

    @Test
    public void testDecimal() {
        assertParsingError("decimal1 + booleanTrue", "Cannot apply '+' to arguments of type '<DECIMAL(38, 38)> + <BOOLEAN>'");

        assertRow("decimal1 + byte1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("decimal1 + byteMax", EXPR0, DECIMAL, BigDecimal.valueOf(1).add(BigDecimal.valueOf(Byte.MAX_VALUE)));
        assertRow("decimal1 + short1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("decimal1 + shortMax", EXPR0, DECIMAL, BigDecimal.valueOf(1).add(BigDecimal.valueOf(Short.MAX_VALUE)));
        assertRow("decimal1 + int1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("decimal1 + intMax", EXPR0, DECIMAL, BigDecimal.valueOf(1).add(BigDecimal.valueOf(Integer.MAX_VALUE)));
        assertRow("decimal1 + long1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("decimal1 + longMax", EXPR0, DECIMAL, BigDecimal.valueOf(1).add(BigDecimal.valueOf(Long.MAX_VALUE)));

        assertRow("decimal1 + float1", EXPR0, REAL, 2.0f);
        assertRow("decimal1 + floatMax", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("decimal1 + double1", EXPR0, DOUBLE, 2.0);
        assertRow("decimal1 + doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("decimal1 + decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("decimal1 + decimalBig", EXPR0, DECIMAL, getRecord().decimal1.add(getRecord().decimalBig));
        assertRow("decimal1 + bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("decimal1 + bigIntegerBig", EXPR0, DECIMAL, getRecord().decimal1.add(getRecord().decimalBig));

        assertRow("decimal1 + string1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("decimal1 + stringBig", EXPR0, DECIMAL, getRecord().decimal1.add(new BigDecimal(getRecord().stringBig)));
        assertDataError("decimal1 + stringFoo", "Cannot convert VARCHAR to DECIMAL: foo");
        assertRow("decimal1 + char1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertDataError("decimal1 + charF", "Cannot convert VARCHAR to DECIMAL: f");

        assertParsingError("decimal1 + object", "Cannot apply '+' to arguments of type '<DECIMAL(38, 38)> + <ANY>'");
    }

    @Test
    public void testVarchar() {
        assertParsingError("string1 + booleanTrue", "Cannot apply '+' to arguments of type '<BOOLEAN> + <BOOLEAN>'");

        assertRow("string1 + byte1", EXPR0, BIGINT, 2L);
        assertRow("string1 + byteMax", EXPR0, BIGINT, 1L + Byte.MAX_VALUE);
        assertRow("string1 + short1", EXPR0, BIGINT, 2L);
        assertRow("string1 + shortMax", EXPR0, BIGINT, 1L + Short.MAX_VALUE);
        assertRow("string1 + int1", EXPR0, BIGINT, 2L);
        assertRow("string1 + intMax", EXPR0, BIGINT, 1L + Integer.MAX_VALUE);
        assertRow("string1 + long1", EXPR0, BIGINT, 2L);
        assertDataError("string1 + longMax", "BIGINT overflow");

        assertRow("string1 + float1", EXPR0, REAL, 2.0f);
        assertRow("string1 + floatMax", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("string1 + double1", EXPR0, DOUBLE, 2.0);
        assertRow("string1 + doubleMax", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("string1 + decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("string1 + decimalBig", EXPR0, DECIMAL, BigDecimal.valueOf(1).add(getRecord().decimalBig));
        assertRow("string1 + bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertRow("string1 + bigIntegerBig", EXPR0, DECIMAL, BigDecimal.valueOf(1).add(getRecord().decimalBig));

        assertRow("string1 + string1", EXPR0, DOUBLE, 2.0);
        assertRow("string1 + stringBig", EXPR0, DOUBLE, 1.0 + Float.parseFloat(getRecord().stringBig));
        assertDataError("string1 + stringFoo", "Cannot convert VARCHAR to DOUBLE: foo");
        assertRow("string1 + char1", EXPR0, DOUBLE, 2.0);
        assertDataError("string1 + charF", "Cannot convert VARCHAR to DOUBLE: f");

        assertParsingError("string1 + object", "Cannot apply '+' to arguments of type '<VARCHAR> + <ANY>'");
    }

    @Test
    public void testObject() {
        assertParsingError("object + booleanTrue", "Cannot apply '+' to arguments of type '<ANY> + <BOOLEAN>'");

        assertParsingError("object + byte1", "Cannot apply '+' to arguments of type '<ANY> + <TINYINT>'");
        assertParsingError("object + short1", "Cannot apply '+' to arguments of type '<ANY> + <SMALLINT>'");
        assertParsingError("object + int1", "Cannot apply '+' to arguments of type '<ANY> + <INTEGER>'");
        assertParsingError("object + long1", "Cannot apply '+' to arguments of type '<ANY> + <BIGINT>'");

        assertParsingError("object + float1", "Cannot apply '+' to arguments of type '<ANY> + <REAL>'");
        assertParsingError("object + double1", "Cannot apply '+' to arguments of type '<ANY> + <DOUBLE>'");

        assertParsingError("object + decimal1", "Cannot apply '+' to arguments of type '<ANY> + <DECIMAL(38, 38)>'");
        assertParsingError("object + bigInteger1", "Cannot apply '+' to arguments of type '<ANY> + <DECIMAL(38, 38)>'");

        assertParsingError("object + string1", "Cannot apply '+' to arguments of type '<ANY> + <VARCHAR>'");
        assertParsingError("object + char1", "Cannot apply '+' to arguments of type '<ANY> + <VARCHAR>'");

        assertParsingError("object + object", "Cannot apply '+' to arguments of type '<ANY> + <ANY>'");
    }

}
