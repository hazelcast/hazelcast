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
public class PlusIntegrationTest extends ExpressionIntegrationTestBase {

    @Test
    public void testBoolean() {
        assertParsingError("booleanTrue + booleanTrue", "No operator matches '<BOOLEAN> + <BOOLEAN>'");

        assertParsingError("booleanTrue + byte1", "No operator matches '<BOOLEAN> + <TINYINT>'");
        assertParsingError("booleanTrue + short1", "No operator matches '<BOOLEAN> + <SMALLINT>'");
        assertParsingError("booleanTrue + int1", "No operator matches '<BOOLEAN> + <INTEGER>'");
        assertParsingError("booleanTrue + long1", "No operator matches '<BOOLEAN> + <BIGINT>'");

        assertParsingError("booleanTrue + float1", "No operator matches '<BOOLEAN> + <REAL>'");
        assertParsingError("booleanTrue + double1", "No operator matches '<BOOLEAN> + <DOUBLE>'");

        assertParsingError("booleanTrue + decimal1", "No operator matches '<BOOLEAN> + <DECIMAL(38, 38)>'");
        assertParsingError("booleanTrue + bigInteger1", "No operator matches '<BOOLEAN> + <DECIMAL(38, 38)>'");

        assertParsingError("booleanTrue + string1", "No operator matches '<BOOLEAN> + <BOOLEAN>'");
        assertParsingError("booleanTrue + char1", "No operator matches '<BOOLEAN> + <BOOLEAN>'");

        assertParsingError("booleanTrue + dateCol", "No operator matches '<BOOLEAN> + <DATE>'");
        assertParsingError("booleanTrue + timeCol", "No operator matches '<BOOLEAN> + <TIME>'");
        assertParsingError("booleanTrue + dateTimeCol", "No operator matches '<BOOLEAN> + <TIMESTAMP>'");
        assertParsingError("booleanTrue + offsetDateTimeCol", "No operator matches '<BOOLEAN> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("booleanTrue + object", "No operator matches '<BOOLEAN> + <OBJECT>'");
    }

    @Test
    public void testTinyint() {
        assertParsingError("byte1 + booleanTrue", "No operator matches '<TINYINT> + <BOOLEAN>'");

        assertRow("byte1 + byte1", EXPR0, SMALLINT, (short) 2);
        assertRow("byte1 + byteMax", EXPR0, SMALLINT, (short) (1 + Byte.MAX_VALUE));
        assertRow("byte1 + short1", EXPR0, INTEGER, 2);
        assertRow("byte1 + shortMax", EXPR0, INTEGER, 1 + Short.MAX_VALUE);
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
        assertDataError("byte1 + stringBig", "Cannot convert VARCHAR to BIGINT");
        assertDataError("byte1 + stringFoo", "Cannot convert VARCHAR to BIGINT");
        assertRow("byte1 + char1", EXPR0, BIGINT, 2L);
        assertDataError("byte1 + charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("byte1 + dateCol", "No operator matches '<TINYINT> + <DATE>'");
        assertParsingError("byte1 + timeCol", "No operator matches '<TINYINT> + <TIME>'");
        assertParsingError("byte1 + dateTimeCol", "No operator matches '<TINYINT> + <TIMESTAMP>'");
        assertParsingError("byte1 + offsetDateTimeCol", "No operator matches '<TINYINT> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("byte1 + object", "No operator matches '<TINYINT> + <OBJECT>'");
    }

    @Test
    public void testSmallint() {
        assertParsingError("short1 + booleanTrue", "No operator matches '<SMALLINT> + <BOOLEAN>'");

        assertRow("short1 + byte1", EXPR0, INTEGER, 2);
        assertRow("short1 + byteMax", EXPR0, INTEGER, 1 + Byte.MAX_VALUE);
        assertRow("short1 + short1", EXPR0, INTEGER, 2);
        assertRow("short1 + shortMax", EXPR0, INTEGER, 1 + Short.MAX_VALUE);
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
        assertDataError("short1 + stringBig", "Cannot convert VARCHAR to BIGINT");
        assertDataError("short1 + stringFoo", "Cannot convert VARCHAR to BIGINT");
        assertRow("short1 + char1", EXPR0, BIGINT, 2L);
        assertDataError("short1 + charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("short1 + dateCol", "No operator matches '<SMALLINT> + <DATE>'");
        assertParsingError("short1 + timeCol", "No operator matches '<SMALLINT> + <TIME>'");
        assertParsingError("short1 + dateTimeCol", "No operator matches '<SMALLINT> + <TIMESTAMP>'");
        assertParsingError("short1 + offsetDateTimeCol", "No operator matches '<SMALLINT> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("short1 + object", "No operator matches '<SMALLINT> + <OBJECT>'");
    }

    @Test
    public void testInteger() {
        assertParsingError("int1 + booleanTrue", "No operator matches '<INTEGER> + <BOOLEAN>'");

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
        assertDataError("int1 + stringBig", "Cannot convert VARCHAR to BIGINT");
        assertDataError("int1 + stringFoo", "Cannot convert VARCHAR to BIGINT");
        assertRow("int1 + char1", EXPR0, BIGINT, 2L);
        assertDataError("int1 + charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("int1 + dateCol", "No operator matches '<INTEGER> + <DATE>'");
        assertParsingError("int1 + timeCol", "No operator matches '<INTEGER> + <TIME>'");
        assertParsingError("int1 + dateTimeCol", "No operator matches '<INTEGER> + <TIMESTAMP>'");
        assertParsingError("int1 + offsetDateTimeCol", "No operator matches '<INTEGER> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("int1 + object", "No operator matches '<INTEGER> + <OBJECT>'");
    }

    @Test
    public void testBigint() {
        assertParsingError("long1 + booleanTrue", "No operator matches '<BIGINT> + <BOOLEAN>'");

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
        assertDataError("long1 + stringBig", "Cannot convert VARCHAR to BIGINT");
        assertDataError("long1 + stringFoo", "Cannot convert VARCHAR to BIGINT");
        assertRow("long1 + char1", EXPR0, BIGINT, 2L);
        assertDataError("long1 + charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("long1 + dateCol", "No operator matches '<BIGINT> + <DATE>'");
        assertParsingError("long1 + timeCol", "No operator matches '<BIGINT> + <TIME>'");
        assertParsingError("long1 + dateTimeCol", "No operator matches '<BIGINT> + <TIMESTAMP>'");
        assertParsingError("long1 + offsetDateTimeCol", "No operator matches '<BIGINT> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("long1 + object", "No operator matches '<BIGINT> + <OBJECT>'");
    }

    @Test
    public void testReal() {
        assertParsingError("float1 + booleanTrue", "No operator matches '<REAL> + <BOOLEAN>'");

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
        assertDataError("float1 + stringFoo", "Cannot convert VARCHAR to REAL");
        assertRow("float1 + char1", EXPR0, REAL, 2.0f);
        assertDataError("float1 + charF", "Cannot convert VARCHAR to REAL");

        assertParsingError("float1 + dateCol", "No operator matches '<REAL> + <DATE>'");
        assertParsingError("float1 + timeCol", "No operator matches '<REAL> + <TIME>'");
        assertParsingError("float1 + dateTimeCol", "No operator matches '<REAL> + <TIMESTAMP>'");
        assertParsingError("float1 + offsetDateTimeCol", "No operator matches '<REAL> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("float1 + object", "No operator matches '<REAL> + <OBJECT>'");
    }

    @Test
    public void testDouble() {
        assertParsingError("double1 + booleanTrue", "No operator matches '<DOUBLE> + <BOOLEAN>'");

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
        assertRow("double1 + decimalBig", EXPR0, DOUBLE, 1.0 + getRecord().decimalBig.doubleValue());
        assertRow("double1 + bigInteger1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + bigIntegerBig", EXPR0, DOUBLE, 1.0 + getRecord().bigIntegerBig.doubleValue());

        assertRow("double1 + string1", EXPR0, DOUBLE, 2.0);
        assertRow("double1 + stringBig", EXPR0, DOUBLE, 1.0 + Double.parseDouble(getRecord().stringBig));
        assertDataError("double1 + stringFoo", "Cannot convert VARCHAR to DOUBLE");
        assertRow("double1 + char1", EXPR0, DOUBLE, 2.0);
        assertDataError("double1 + charF", "Cannot convert VARCHAR to DOUBLE");

        assertParsingError("double1 + dateCol", "No operator matches '<DOUBLE> + <DATE>'");
        assertParsingError("double1 + timeCol", "No operator matches '<DOUBLE> + <TIME>'");
        assertParsingError("double1 + dateTimeCol", "No operator matches '<DOUBLE> + <TIMESTAMP>'");
        assertParsingError("double1 + offsetDateTimeCol", "No operator matches '<DOUBLE> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("double1 + object", "No operator matches '<DOUBLE> + <OBJECT>'");
    }

    @Test
    public void testDecimal() {
        assertParsingError("decimal1 + booleanTrue", "No operator matches '<DECIMAL(38, 38)> + <BOOLEAN>'");

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
        assertDataError("decimal1 + stringFoo", "Cannot convert VARCHAR to DECIMAL");
        assertRow("decimal1 + char1", EXPR0, DECIMAL, BigDecimal.valueOf(2));
        assertDataError("decimal1 + charF", "Cannot convert VARCHAR to DECIMAL");

        assertParsingError("decimal1 + dateCol", "No operator matches '<DECIMAL(38, 38)> + <DATE>'");
        assertParsingError("decimal1 + timeCol", "No operator matches '<DECIMAL(38, 38)> + <TIME>'");
        assertParsingError("decimal1 + dateTimeCol", "No operator matches '<DECIMAL(38, 38)> + <TIMESTAMP>'");
        assertParsingError("decimal1 + offsetDateTimeCol", "No operator matches '<DECIMAL(38, 38)> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("decimal1 + object", "No operator matches '<DECIMAL(38, 38)> + <OBJECT>'");
    }

    @Test
    public void testVarchar() {
        assertParsingError("string1 + booleanTrue", "No operator matches '<BOOLEAN> + <BOOLEAN>'");

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
        assertRow("string1 + stringBig", EXPR0, DOUBLE, 1.0 + Double.parseDouble(getRecord().stringBig));
        assertDataError("string1 + stringFoo", "Cannot convert VARCHAR to DOUBLE");
        assertRow("string1 + char1", EXPR0, DOUBLE, 2.0);
        assertDataError("string1 + charF", "Cannot convert VARCHAR to DOUBLE");

        assertParsingError("string1 + dateCol", "No operator matches '<DATE> + <DATE>'");
        assertParsingError("string1 + timeCol", "No operator matches '<TIME> + <TIME>'");
        assertParsingError("string1 + dateTimeCol", "No operator matches '<TIMESTAMP> + <TIMESTAMP>'");
        assertParsingError("string1 + offsetDateTimeCol", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("string1 + object", "No operator matches '<VARCHAR> + <OBJECT>'");
    }

    @Test
    public void testDate() {
        assertParsingError("dateCol + booleanTrue", "No operator matches '<DATE> + <BOOLEAN>'");

        assertParsingError("dateCol + byte1", "No operator matches '<DATE> + <TINYINT>'");
        assertParsingError("dateCol + short1", "No operator matches '<DATE> + <SMALLINT>'");
        assertParsingError("dateCol + int1", "No operator matches '<DATE> + <INTEGER>'");
        assertParsingError("dateCol + long1", "No operator matches '<DATE> + <BIGINT>'");

        assertParsingError("dateCol + float1", "No operator matches '<DATE> + <REAL>'");
        assertParsingError("dateCol + double1", "No operator matches '<DATE> + <DOUBLE>'");

        assertParsingError("dateCol + decimal1", "No operator matches '<DATE> + <DECIMAL(38, 38)>'");
        assertParsingError("dateCol + bigInteger1", "No operator matches '<DATE> + <DECIMAL(38, 38)>'");

        assertParsingError("dateCol + string1", "No operator matches '<DATE> + <DATE>'");
        assertParsingError("dateCol + char1", "No operator matches '<DATE> + <DATE>'");

        assertParsingError("dateCol + dateCol", "No operator matches '<DATE> + <DATE>'");
        assertParsingError("dateCol + timeCol", "No operator matches '<DATE> + <TIME>'");
        assertParsingError("dateCol + dateTimeCol", "No operator matches '<DATE> + <TIMESTAMP>'");
        assertParsingError("dateCol + offsetDateTimeCol", "No operator matches '<DATE> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("dateCol + object", "No operator matches '<DATE> + <OBJECT>'");
    }

    @Test
    public void testTime() {
        assertParsingError("timeCol + booleanTrue", "No operator matches '<TIME> + <BOOLEAN>'");

        assertParsingError("timeCol + byte1", "No operator matches '<TIME> + <TINYINT>'");
        assertParsingError("timeCol + short1", "No operator matches '<TIME> + <SMALLINT>'");
        assertParsingError("timeCol + int1", "No operator matches '<TIME> + <INTEGER>'");
        assertParsingError("timeCol + long1", "No operator matches '<TIME> + <BIGINT>'");

        assertParsingError("timeCol + float1", "No operator matches '<TIME> + <REAL>'");
        assertParsingError("timeCol + double1", "No operator matches '<TIME> + <DOUBLE>'");

        assertParsingError("timeCol + decimal1", "No operator matches '<TIME> + <DECIMAL(38, 38)>'");
        assertParsingError("timeCol + bigInteger1", "No operator matches '<TIME> + <DECIMAL(38, 38)>'");

        assertParsingError("timeCol + string1", "No operator matches '<TIME> + <TIME>'");
        assertParsingError("timeCol + char1", "No operator matches '<TIME> + <TIME>'");

        assertParsingError("timeCol + dateCol", "No operator matches '<TIME> + <DATE>'");
        assertParsingError("timeCol + timeCol", "No operator matches '<TIME> + <TIME>'");
        assertParsingError("timeCol + dateTimeCol", "No operator matches '<TIME> + <TIMESTAMP>'");
        assertParsingError("timeCol + offsetDateTimeCol", "No operator matches '<TIME> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("timeCol + object", "No operator matches '<TIME> + <OBJECT>'");
    }

    @Test
    public void testTimestamp() {
        assertParsingError("dateTimeCol + booleanTrue", "No operator matches '<TIMESTAMP> + <BOOLEAN>'");

        assertParsingError("dateTimeCol + byte1", "No operator matches '<TIMESTAMP> + <TINYINT>'");
        assertParsingError("dateTimeCol + short1", "No operator matches '<TIMESTAMP> + <SMALLINT>'");
        assertParsingError("dateTimeCol + int1", "No operator matches '<TIMESTAMP> + <INTEGER>'");
        assertParsingError("dateTimeCol + long1", "No operator matches '<TIMESTAMP> + <BIGINT>'");

        assertParsingError("dateTimeCol + float1", "No operator matches '<TIMESTAMP> + <REAL>'");
        assertParsingError("dateTimeCol + double1", "No operator matches '<TIMESTAMP> + <DOUBLE>'");

        assertParsingError("dateTimeCol + decimal1", "No operator matches '<TIMESTAMP> + <DECIMAL(38, 38)>'");
        assertParsingError("dateTimeCol + bigInteger1", "No operator matches '<TIMESTAMP> + <DECIMAL(38, 38)>'");

        assertParsingError("dateTimeCol + string1", "No operator matches '<TIMESTAMP> + <TIMESTAMP>'");
        assertParsingError("dateTimeCol + char1", "No operator matches '<TIMESTAMP> + <TIMESTAMP>'");

        assertParsingError("dateTimeCol + dateCol", "No operator matches '<TIMESTAMP> + <DATE>'");
        assertParsingError("dateTimeCol + timeCol", "No operator matches '<TIMESTAMP> + <TIME>'");
        assertParsingError("dateTimeCol + dateTimeCol", "No operator matches '<TIMESTAMP> + <TIMESTAMP>'");
        assertParsingError("dateTimeCol + offsetDateTimeCol", "No operator matches '<TIMESTAMP> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("dateTimeCol + object", "No operator matches '<TIMESTAMP> + <OBJECT>'");
    }

    @Test
    public void testTimestampWithTimeZone() {
        assertParsingError("offsetDateTimeCol + booleanTrue", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <BOOLEAN>'");

        assertParsingError("offsetDateTimeCol + byte1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <TINYINT>'");
        assertParsingError("offsetDateTimeCol + short1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <SMALLINT>'");
        assertParsingError("offsetDateTimeCol + int1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <INTEGER>'");
        assertParsingError("offsetDateTimeCol + long1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <BIGINT>'");

        assertParsingError("offsetDateTimeCol + float1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <REAL>'");
        assertParsingError("offsetDateTimeCol + double1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <DOUBLE>'");

        assertParsingError("offsetDateTimeCol + decimal1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <DECIMAL(38, 38)>'");
        assertParsingError("offsetDateTimeCol + bigInteger1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <DECIMAL(38, 38)>'");

        assertParsingError("offsetDateTimeCol + string1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <TIMESTAMP_WITH_TIME_ZONE>'");
        assertParsingError("offsetDateTimeCol + char1", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("offsetDateTimeCol + dateCol", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <DATE>'");
        assertParsingError("offsetDateTimeCol + timeCol", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <TIME>'");
        assertParsingError("offsetDateTimeCol + dateTimeCol", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <TIMESTAMP>'");
        assertParsingError("offsetDateTimeCol + offsetDateTimeCol", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("offsetDateTimeCol + object", "No operator matches '<TIMESTAMP_WITH_TIME_ZONE> + <OBJECT>'");
    }

    @Test
    public void testObject() {
        assertParsingError("object + booleanTrue", "No operator matches '<OBJECT> + <BOOLEAN>'");

        assertParsingError("object + byte1", "No operator matches '<OBJECT> + <TINYINT>'");
        assertParsingError("object + short1", "No operator matches '<OBJECT> + <SMALLINT>'");
        assertParsingError("object + int1", "No operator matches '<OBJECT> + <INTEGER>'");
        assertParsingError("object + long1", "No operator matches '<OBJECT> + <BIGINT>'");

        assertParsingError("object + float1", "No operator matches '<OBJECT> + <REAL>'");
        assertParsingError("object + double1", "No operator matches '<OBJECT> + <DOUBLE>'");

        assertParsingError("object + decimal1", "No operator matches '<OBJECT> + <DECIMAL(38, 38)>'");
        assertParsingError("object + bigInteger1", "No operator matches '<OBJECT> + <DECIMAL(38, 38)>'");

        assertParsingError("object + string1", "No operator matches '<OBJECT> + <VARCHAR>'");
        assertParsingError("object + char1", "No operator matches '<OBJECT> + <VARCHAR>'");

        assertParsingError("object + dateCol", "No operator matches '<OBJECT> + <DATE>'");
        assertParsingError("object + timeCol", "No operator matches '<OBJECT> + <TIME>'");
        assertParsingError("object + dateTimeCol", "No operator matches '<OBJECT> + <TIMESTAMP>'");
        assertParsingError("object + offsetDateTimeCol", "No operator matches '<OBJECT> + <TIMESTAMP_WITH_TIME_ZONE>'");

        assertParsingError("object + object", "No operator matches '<OBJECT> + <OBJECT>'");
    }
}
