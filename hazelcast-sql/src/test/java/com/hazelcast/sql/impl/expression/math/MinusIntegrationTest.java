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
public class MinusIntegrationTest extends ExpressionIntegrationTestBase {

    @Test
    public void testBoolean() {
        assertParsingError("booleanTrue - booleanTrue", "Cannot apply [BOOLEAN, BOOLEAN] to the '-' operator");

        assertParsingError("booleanTrue - byte1", "Cannot apply [BOOLEAN, TINYINT] to the '-' operator");
        assertParsingError("booleanTrue - short1", "Cannot apply [BOOLEAN, SMALLINT] to the '-' operator");
        assertParsingError("booleanTrue - int1", "Cannot apply [BOOLEAN, INTEGER] to the '-' operator");
        assertParsingError("booleanTrue - long1", "Cannot apply [BOOLEAN, BIGINT] to the '-' operator");

        assertParsingError("booleanTrue - float1", "Cannot apply [BOOLEAN, REAL] to the '-' operator");
        assertParsingError("booleanTrue - double1", "Cannot apply [BOOLEAN, DOUBLE] to the '-' operator");

        assertParsingError("booleanTrue - decimal1", "Cannot apply [BOOLEAN, DECIMAL] to the '-' operator");
        assertParsingError("booleanTrue - bigInteger1", "Cannot apply [BOOLEAN, DECIMAL] to the '-' operator");

        assertParsingError("booleanTrue - string1", "Cannot apply [BOOLEAN, VARCHAR] to the '-' operator");
        assertParsingError("booleanTrue - char1", "Cannot apply [BOOLEAN, VARCHAR] to the '-' operator");

        assertParsingError("booleanTrue - dateCol", "Cannot apply [BOOLEAN, DATE] to the '-' operator");
        assertParsingError("booleanTrue - timeCol", "Cannot apply [BOOLEAN, TIME] to the '-' operator");
        assertParsingError("booleanTrue - dateTimeCol", "Cannot apply [BOOLEAN, TIMESTAMP] to the '-' operator");
        assertParsingError("booleanTrue - offsetDateTimeCol", "Cannot apply [BOOLEAN, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("booleanTrue - object", "Cannot apply [BOOLEAN, OBJECT] to the '-' operator");
    }

    @Test
    public void testTinyint() {
        assertParsingError("byte1 - booleanTrue", "Cannot apply [TINYINT, BOOLEAN] to the '-' operator");

        assertRow("byte1 - byte1", EXPR0, SMALLINT, (short) 0);
        assertRow("byte1 - byteMin", EXPR0, SMALLINT, (short) (1 - Byte.MIN_VALUE));
        assertRow("byte1 - short1", EXPR0, INTEGER, 0);
        assertRow("byte1 - shortMin", EXPR0, INTEGER, 1 - Short.MIN_VALUE);
        assertRow("byte1 - int1", EXPR0, BIGINT, 0L);
        assertRow("byte1 - intMin", EXPR0, BIGINT, 1L - Integer.MIN_VALUE);
        assertRow("byte1 - long1", EXPR0, BIGINT, 0L);
        assertDataError("byte1 - longMin", "BIGINT overflow");

        assertRow("byte1 - float1", EXPR0, REAL, 0.0f);
        assertRow("byte1 - floatMin", EXPR0, REAL, 1 - Float.MIN_VALUE);
        assertRow("byte1 - double1", EXPR0, DOUBLE, 0.0);
        assertRow("byte1 - doubleMin", EXPR0, DOUBLE, 1 - Double.MIN_VALUE);

        assertRow("byte1 - decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("byte1 - decimalBigNegative", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));
        assertRow("byte1 - bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("byte1 - bigIntegerBigNegative", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));

        assertParsingError("byte1 - string1", "Cannot apply [TINYINT, VARCHAR] to the '-' operator");
        assertParsingError("byte1 - char1", "Cannot apply [TINYINT, VARCHAR] to the '-' operator");
        assertParsingError("byte1 - dateCol", "Cannot apply [TINYINT, DATE] to the '-' operator");
        assertParsingError("byte1 - timeCol", "Cannot apply [TINYINT, TIME] to the '-' operator");
        assertParsingError("byte1 - dateTimeCol", "Cannot apply [TINYINT, TIMESTAMP] to the '-' operator");
        assertParsingError("byte1 - offsetDateTimeCol", "Cannot apply [TINYINT, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("byte1 - object", "Cannot apply [TINYINT, OBJECT] to the '-' operator");
    }

    @Test
    public void testSmallint() {
        assertParsingError("short1 - booleanTrue", "Cannot apply [SMALLINT, BOOLEAN] to the '-' operator");

        assertRow("short1 - byte1", EXPR0, INTEGER, 0);
        assertRow("short1 - byteMin", EXPR0, INTEGER, 1 - Byte.MIN_VALUE);
        assertRow("short1 - short1", EXPR0, INTEGER, 0);
        assertRow("short1 - shortMin", EXPR0, INTEGER, 1 - Short.MIN_VALUE);
        assertRow("short1 - int1", EXPR0, BIGINT, 0L);
        assertRow("short1 - intMin", EXPR0, BIGINT, 1L - Integer.MIN_VALUE);
        assertRow("short1 - long1", EXPR0, BIGINT, 0L);
        assertDataError("short1 - longMin", "BIGINT overflow");

        assertRow("short1 - float1", EXPR0, REAL, 0.0f);
        assertRow("short1 - floatMin", EXPR0, REAL, 1 - Float.MIN_VALUE);
        assertRow("short1 - double1", EXPR0, DOUBLE, 0.0);
        assertRow("short1 - doubleMin", EXPR0, DOUBLE, 1 - Double.MIN_VALUE);

        assertRow("short1 - decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("short1 - decimalBigNegative", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));
        assertRow("short1 - bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("short1 - bigIntegerBigNegative", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));

        assertParsingError("short1 - string1", "Cannot apply [SMALLINT, VARCHAR] to the '-' operator");
        assertParsingError("short1 - char1", "Cannot apply [SMALLINT, VARCHAR] to the '-' operator");
        assertParsingError("short1 - dateCol", "Cannot apply [SMALLINT, DATE] to the '-' operator");
        assertParsingError("short1 - timeCol", "Cannot apply [SMALLINT, TIME] to the '-' operator");
        assertParsingError("short1 - dateTimeCol", "Cannot apply [SMALLINT, TIMESTAMP] to the '-' operator");
        assertParsingError("short1 - offsetDateTimeCol", "Cannot apply [SMALLINT, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("short1 - object", "Cannot apply [SMALLINT, OBJECT] to the '-' operator");
    }

    @Test
    public void testInteger() {
        assertParsingError("int1 - booleanTrue", "Cannot apply [INTEGER, BOOLEAN] to the '-' operator");

        assertRow("int1 - byte1", EXPR0, BIGINT, 0L);
        assertRow("int1 - byteMin", EXPR0, BIGINT, 1L - Byte.MIN_VALUE);
        assertRow("int1 - short1", EXPR0, BIGINT, 0L);
        assertRow("int1 - shortMin", EXPR0, BIGINT, 1L - Short.MIN_VALUE);
        assertRow("int1 - int1", EXPR0, BIGINT, 0L);
        assertRow("int1 - intMin", EXPR0, BIGINT, 1L - Integer.MIN_VALUE);
        assertRow("int1 - long1", EXPR0, BIGINT, 0L);
        assertDataError("int1 - longMin", "BIGINT overflow");

        assertRow("int1 - float1", EXPR0, REAL, 0.0f);
        assertRow("int1 - floatMin", EXPR0, REAL, 1 - Float.MIN_VALUE);
        assertRow("int1 - double1", EXPR0, DOUBLE, 0.0);
        assertRow("int1 - doubleMin", EXPR0, DOUBLE, 1 - Double.MIN_VALUE);

        assertRow("int1 - decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("int1 - decimalBigNegative", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));
        assertRow("int1 - bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("int1 - bigIntegerBigNegative", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));

        assertParsingError("int1 - string1", "Cannot apply [INTEGER, VARCHAR] to the '-' operator");
        assertParsingError("int1 - char1", "Cannot apply [INTEGER, VARCHAR] to the '-' operator");
        assertParsingError("int1 - dateCol", "Cannot apply [INTEGER, DATE] to the '-' operator");
        assertParsingError("int1 - timeCol", "Cannot apply [INTEGER, TIME] to the '-' operator");
        assertParsingError("int1 - dateTimeCol", "Cannot apply [INTEGER, TIMESTAMP] to the '-' operator");
        assertParsingError("int1 - offsetDateTimeCol", "Cannot apply [INTEGER, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");
        assertParsingError("int1 - object", "Cannot apply [INTEGER, OBJECT] to the '-' operator");
    }

    @Test
    public void testBigint() {
        assertParsingError("long1 - booleanTrue", "Cannot apply [BIGINT, BOOLEAN] to the '-' operator");

        assertRow("long1 - byte1", EXPR0, BIGINT, 0L);
        assertRow("long1 - byteMin", EXPR0, BIGINT, 1L - Byte.MIN_VALUE);
        assertRow("long1 - short1", EXPR0, BIGINT, 0L);
        assertRow("long1 - shortMin", EXPR0, BIGINT, 1L - Short.MIN_VALUE);
        assertRow("long1 - int1", EXPR0, BIGINT, 0L);
        assertRow("long1 - intMin", EXPR0, BIGINT, 1L - Integer.MIN_VALUE);
        assertRow("long1 - long1", EXPR0, BIGINT, 0L);
        assertDataError("long1 - longMin", "BIGINT overflow");

        assertRow("long1 - float1", EXPR0, REAL, 0.0f);
        assertRow("long1 - floatMin", EXPR0, REAL, 1 - Float.MIN_VALUE);
        assertRow("long1 - double1", EXPR0, DOUBLE, 0.0);
        assertRow("long1 - doubleMin", EXPR0, DOUBLE, 1 - Double.MIN_VALUE);

        assertRow("long1 - decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("long1 - decimalBigNegative", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));
        assertRow("long1 - bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("long1 - bigIntegerBigNegative", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));

        assertParsingError("long1 - string1", "Cannot apply [BIGINT, VARCHAR] to the '-' operator");
        assertParsingError("long1 - char1", "Cannot apply [BIGINT, VARCHAR] to the '-' operator");
        assertParsingError("long1 - dateCol", "Cannot apply [BIGINT, DATE] to the '-' operator");
        assertParsingError("long1 - timeCol", "Cannot apply [BIGINT, TIME] to the '-' operator");
        assertParsingError("long1 - dateTimeCol", "Cannot apply [BIGINT, TIMESTAMP] to the '-' operator");
        assertParsingError("long1 - offsetDateTimeCol", "Cannot apply [BIGINT, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");
        assertParsingError("long1 - object", "Cannot apply [BIGINT, OBJECT] to the '-' operator");
    }

    @Test
    public void testReal() {
        assertParsingError("float1 - booleanTrue", "Cannot apply [REAL, BOOLEAN] to the '-' operator");

        assertRow("float1 - byte1", EXPR0, REAL, 0.0f);
        assertRow("float1 - byteMin", EXPR0, REAL, 1.0f - Byte.MIN_VALUE);
        assertRow("float1 - short1", EXPR0, REAL, 0.0f);
        assertRow("float1 - shortMin", EXPR0, REAL, 1.0f - Short.MIN_VALUE);
        assertRow("float1 - int1", EXPR0, REAL, 0.0f);
        assertRow("float1 - intMin", EXPR0, REAL, 1.0f - Integer.MIN_VALUE);
        assertRow("float1 - long1", EXPR0, REAL, 0.0f);
        assertRow("float1 - longMin", EXPR0, REAL, 1.0f - Long.MIN_VALUE);

        assertRow("float1 - float1", EXPR0, REAL, 0.0f);
        assertRow("float1 - floatMin", EXPR0, REAL, 1 - Float.MIN_VALUE);
        assertRow("float1 - double1", EXPR0, DOUBLE, 0.0);
        assertRow("float1 - doubleMin", EXPR0, DOUBLE, 1 - Double.MIN_VALUE);

        assertRow("float1 - decimal1", EXPR0, REAL, 0.0f);
        assertRow("float1 - decimalBigNegative", EXPR0, REAL, 1.0f - getRecord().decimalBigNegative.floatValue());
        assertRow("float1 - bigInteger1", EXPR0, REAL, 0.0f);
        assertRow("float1 - bigIntegerBigNegative", EXPR0, REAL, 1.0f - getRecord().bigIntegerBigNegative.floatValue());

        assertParsingError("float1 - string1", "Cannot apply [REAL, VARCHAR] to the '-' operator");
        assertParsingError("float1 - char1", "Cannot apply [REAL, VARCHAR] to the '-' operator");
        assertParsingError("float1 - dateCol", "Cannot apply [REAL, DATE] to the '-' operator");
        assertParsingError("float1 - timeCol", "Cannot apply [REAL, TIME] to the '-' operator");
        assertParsingError("float1 - dateTimeCol", "Cannot apply [REAL, TIMESTAMP] to the '-' operator");
        assertParsingError("float1 - offsetDateTimeCol", "Cannot apply [REAL, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");
        assertParsingError("float1 - object", "Cannot apply [REAL, OBJECT] to the '-' operator");
    }

    @Test
    public void testDouble() {
        assertParsingError("double1 - booleanTrue", "Cannot apply [DOUBLE, BOOLEAN] to the '-' operator");

        assertRow("double1 - byte1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - byteMin", EXPR0, DOUBLE, 1.0 - Byte.MIN_VALUE);
        assertRow("double1 - short1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - shortMin", EXPR0, DOUBLE, 1.0 - Short.MIN_VALUE);
        assertRow("double1 - int1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - intMin", EXPR0, DOUBLE, 1.0 - Integer.MIN_VALUE);
        assertRow("double1 - long1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - longMin", EXPR0, DOUBLE, 1.0 - Long.MIN_VALUE);

        assertRow("double1 - float1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - floatMin", EXPR0, DOUBLE, 1.0 - Float.MIN_VALUE);
        assertRow("double1 - double1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - doubleMin", EXPR0, DOUBLE, 1.0 - Double.MIN_VALUE);

        assertRow("double1 - decimal1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - decimalBigNegative", EXPR0, DOUBLE, 1.0 - getRecord().decimalBigNegative.doubleValue());
        assertRow("double1 - bigInteger1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - bigIntegerBigNegative", EXPR0, DOUBLE, 1.0 - getRecord().bigIntegerBigNegative.doubleValue());

        assertParsingError("double1 - string1", "Cannot apply [DOUBLE, VARCHAR] to the '-' operator");
        assertParsingError("double1 - char1", "Cannot apply [DOUBLE, VARCHAR] to the '-' operator");
        assertParsingError("double1 - dateCol", "Cannot apply [DOUBLE, DATE] to the '-' operator");
        assertParsingError("double1 - timeCol", "Cannot apply [DOUBLE, TIME] to the '-' operator");
        assertParsingError("double1 - dateTimeCol", "Cannot apply [DOUBLE, TIMESTAMP] to the '-' operator");
        assertParsingError("double1 - offsetDateTimeCol", "Cannot apply [DOUBLE, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("double1 - object", "Cannot apply [DOUBLE, OBJECT] to the '-' operator");
    }

    @Test
    public void testDecimal() {
        assertParsingError("decimal1 - booleanTrue", "Cannot apply [DECIMAL, BOOLEAN] to the '-' operator");

        assertRow("decimal1 - byte1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("decimal1 - byteMin", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(BigDecimal.valueOf(Byte.MIN_VALUE)));
        assertRow("decimal1 - short1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("decimal1 - shortMin", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(BigDecimal.valueOf(Short.MIN_VALUE)));
        assertRow("decimal1 - int1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("decimal1 - intMin", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(BigDecimal.valueOf(Integer.MIN_VALUE)));
        assertRow("decimal1 - long1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("decimal1 - longMin", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(BigDecimal.valueOf(Long.MIN_VALUE)));

        assertRow("decimal1 - float1", EXPR0, REAL, 0.0f);
        assertRow("decimal1 - floatMin", EXPR0, REAL, 1.0f - Float.MIN_VALUE);
        assertRow("decimal1 - double1", EXPR0, DOUBLE, 0.0);
        assertRow("decimal1 - doubleMin", EXPR0, DOUBLE, 1.0 - Double.MIN_VALUE);

        assertRow("decimal1 - decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("decimal1 - decimalBigNegative", EXPR0, DECIMAL, getRecord().decimal1.subtract(getRecord().decimalBigNegative));
        assertRow("decimal1 - bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("decimal1 - bigIntegerBigNegative", EXPR0, DECIMAL,
                getRecord().decimal1.subtract(getRecord().decimalBigNegative));

        assertParsingError("decimal1 - string1", "Cannot apply [DECIMAL, VARCHAR] to the '-' operator");
        assertParsingError("decimal1 - char1", "Cannot apply [DECIMAL, VARCHAR] to the '-' operator");
        assertParsingError("decimal1 - dateCol", "Cannot apply [DECIMAL, DATE] to the '-' operator");
        assertParsingError("decimal1 - timeCol", "Cannot apply [DECIMAL, TIME] to the '-' operator");
        assertParsingError("decimal1 - dateTimeCol", "Cannot apply [DECIMAL, TIMESTAMP] to the '-' operator");
        assertParsingError("decimal1 - offsetDateTimeCol", "Cannot apply [DECIMAL, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");
        assertParsingError("decimal1 - object", "Cannot apply [DECIMAL, OBJECT] to the '-' operator");
    }

    @Test
    public void testVarchar() {
        assertParsingError("string1 - booleanTrue", "Cannot apply [VARCHAR, BOOLEAN] to the '-' operator");
        assertParsingError("string1 - byte1", "Cannot apply [VARCHAR, TINYINT] to the '-' operator");
        assertParsingError("string1 - short1", "Cannot apply [VARCHAR, SMALLINT] to the '-' operator");
        assertParsingError("string1 - int1", "Cannot apply [VARCHAR, INTEGER] to the '-' operator");
        assertParsingError("string1 - long1", "Cannot apply [VARCHAR, BIGINT] to the '-' operator");
        assertParsingError("string1 - float1", "Cannot apply [VARCHAR, REAL] to the '-' operator");
        assertParsingError("string1 - double1", "Cannot apply [VARCHAR, DOUBLE] to the '-' operator");
        assertParsingError("string1 - decimal1", "Cannot apply [VARCHAR, DECIMAL] to the '-' operator");
        assertParsingError("string1 - bigInteger1", "Cannot apply [VARCHAR, DECIMAL] to the '-' operator");
        assertParsingError("string1 - string1", "Cannot apply [VARCHAR, VARCHAR] to the '-' operator");
        assertParsingError("string1 - char1", "Cannot apply [VARCHAR, VARCHAR] to the '-' operator");
        assertParsingError("string1 - dateCol", "Cannot apply [VARCHAR, DATE] to the '-' operator");
        assertParsingError("string1 - timeCol", "Cannot apply [VARCHAR, TIME] to the '-' operator");
        assertParsingError("string1 - dateTimeCol", "Cannot apply [VARCHAR, TIMESTAMP] to the '-' operator");
        assertParsingError("string1 - offsetDateTimeCol", "Cannot apply [VARCHAR, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");
        assertParsingError("string1 - object", "Cannot apply [VARCHAR, OBJECT] to the '-' operator");
    }

    @Test
    public void testDate() {
        assertParsingError("dateCol - booleanTrue", "Cannot apply [DATE, BOOLEAN] to the '-' operator");

        assertParsingError("dateCol - byte1", "Cannot apply [DATE, TINYINT] to the '-' operator");
        assertParsingError("dateCol - short1", "Cannot apply [DATE, SMALLINT] to the '-' operator");
        assertParsingError("dateCol - int1", "Cannot apply [DATE, INTEGER] to the '-' operator");
        assertParsingError("dateCol - long1", "Cannot apply [DATE, BIGINT] to the '-' operator");

        assertParsingError("dateCol - float1", "Cannot apply [DATE, REAL] to the '-' operator");
        assertParsingError("dateCol - double1", "Cannot apply [DATE, DOUBLE] to the '-' operator");

        assertParsingError("dateCol - decimal1", "Cannot apply [DATE, DECIMAL] to the '-' operator");
        assertParsingError("dateCol - bigInteger1", "Cannot apply [DATE, DECIMAL] to the '-' operator");

        assertParsingError("dateCol - string1", "Cannot apply [DATE, VARCHAR] to the '-' operator");
        assertParsingError("dateCol - char1", "Cannot apply [DATE, VARCHAR] to the '-' operator");

        assertParsingError("dateCol - dateCol", "Cannot apply [DATE, DATE] to the '-' operator");
        assertParsingError("dateCol - timeCol", "Cannot apply [DATE, TIME] to the '-' operator");
        assertParsingError("dateCol - dateTimeCol", "Cannot apply [DATE, TIMESTAMP] to the '-' operator");
        assertParsingError("dateCol - offsetDateTimeCol", "Cannot apply [DATE, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("dateCol - object", "Cannot apply [DATE, OBJECT] to the '-' operator");
    }

    @Test
    public void testTime() {
        assertParsingError("timeCol - booleanTrue", "Cannot apply [TIME, BOOLEAN] to the '-' operator");

        assertParsingError("timeCol - byte1", "Cannot apply [TIME, TINYINT] to the '-' operator");
        assertParsingError("timeCol - short1", "Cannot apply [TIME, SMALLINT] to the '-' operator");
        assertParsingError("timeCol - int1", "Cannot apply [TIME, INTEGER] to the '-' operator");
        assertParsingError("timeCol - long1", "Cannot apply [TIME, BIGINT] to the '-' operator");

        assertParsingError("timeCol - float1", "Cannot apply [TIME, REAL] to the '-' operator");
        assertParsingError("timeCol - double1", "Cannot apply [TIME, DOUBLE] to the '-' operator");

        assertParsingError("timeCol - decimal1", "Cannot apply [TIME, DECIMAL] to the '-' operator");
        assertParsingError("timeCol - bigInteger1", "Cannot apply [TIME, DECIMAL] to the '-' operator");

        assertParsingError("timeCol - string1", "Cannot apply [TIME, VARCHAR] to the '-' operator");
        assertParsingError("timeCol - char1", "Cannot apply [TIME, VARCHAR] to the '-' operator");

        assertParsingError("timeCol - dateCol", "Cannot apply [TIME, DATE] to the '-' operator");
        assertParsingError("timeCol - timeCol", "Cannot apply [TIME, TIME] to the '-' operator");
        assertParsingError("timeCol - dateTimeCol", "Cannot apply [TIME, TIMESTAMP] to the '-' operator");
        assertParsingError("timeCol - offsetDateTimeCol", "Cannot apply [TIME, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("timeCol - object", "Cannot apply [TIME, OBJECT] to the '-' operator");
    }

    @Test
    public void testTimestamp() {
        assertParsingError("dateTimeCol - booleanTrue", "Cannot apply [TIMESTAMP, BOOLEAN] to the '-' operator");

        assertParsingError("dateTimeCol - byte1", "Cannot apply [TIMESTAMP, TINYINT] to the '-' operator");
        assertParsingError("dateTimeCol - short1", "Cannot apply [TIMESTAMP, SMALLINT] to the '-' operator");
        assertParsingError("dateTimeCol - int1", "Cannot apply [TIMESTAMP, INTEGER] to the '-' operator");
        assertParsingError("dateTimeCol - long1", "Cannot apply [TIMESTAMP, BIGINT] to the '-' operator");

        assertParsingError("dateTimeCol - float1", "Cannot apply [TIMESTAMP, REAL] to the '-' operator");
        assertParsingError("dateTimeCol - double1", "Cannot apply [TIMESTAMP, DOUBLE] to the '-' operator");

        assertParsingError("dateTimeCol - decimal1", "Cannot apply [TIMESTAMP, DECIMAL] to the '-' operator");
        assertParsingError("dateTimeCol - bigInteger1", "Cannot apply [TIMESTAMP, DECIMAL] to the '-' operator");

        assertParsingError("dateTimeCol - string1", "Cannot apply [TIMESTAMP, VARCHAR] to the '-' operator");
        assertParsingError("dateTimeCol - char1", "Cannot apply [TIMESTAMP, VARCHAR] to the '-' operator");

        assertParsingError("dateTimeCol - dateCol", "Cannot apply [TIMESTAMP, DATE] to the '-' operator");
        assertParsingError("dateTimeCol - timeCol", "Cannot apply [TIMESTAMP, TIME] to the '-' operator");
        assertParsingError("dateTimeCol - dateTimeCol", "Cannot apply [TIMESTAMP, TIMESTAMP] to the '-' operator");
        assertParsingError("dateTimeCol - offsetDateTimeCol", "Cannot apply [TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("dateTimeCol - object", "Cannot apply [TIMESTAMP, OBJECT] to the '-' operator");
    }

    @Test
    public void testTimestampWithTimeZone() {
        assertParsingError("offsetDateTimeCol - booleanTrue", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, BOOLEAN] to the '-' operator");

        assertParsingError("offsetDateTimeCol - byte1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, TINYINT] to the '-' operator");
        assertParsingError("offsetDateTimeCol - short1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, SMALLINT] to the '-' operator");
        assertParsingError("offsetDateTimeCol - int1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, INTEGER] to the '-' operator");
        assertParsingError("offsetDateTimeCol - long1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, BIGINT] to the '-' operator");

        assertParsingError("offsetDateTimeCol - float1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, REAL] to the '-' operator");
        assertParsingError("offsetDateTimeCol - double1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, DOUBLE] to the '-' operator");

        assertParsingError("offsetDateTimeCol - decimal1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, DECIMAL] to the '-' operator");
        assertParsingError("offsetDateTimeCol - bigInteger1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, DECIMAL] to the '-' operator");

        assertParsingError("offsetDateTimeCol - string1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, VARCHAR] to the '-' operator");
        assertParsingError("offsetDateTimeCol - char1", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, VARCHAR] to the '-' operator");

        assertParsingError("offsetDateTimeCol - dateCol", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, DATE] to the '-' operator");
        assertParsingError("offsetDateTimeCol - timeCol", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, TIME] to the '-' operator");
        assertParsingError("offsetDateTimeCol - dateTimeCol", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP] to the '-' operator");
        assertParsingError("offsetDateTimeCol - offsetDateTimeCol", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("offsetDateTimeCol - object", "Cannot apply [TIMESTAMP_WITH_TIME_ZONE, OBJECT] to the '-' operator");
    }

    @Test
    public void testObject() {
        assertParsingError("object - booleanTrue", "Cannot apply [OBJECT, BOOLEAN] to the '-' operator");

        assertParsingError("object - byte1", "Cannot apply [OBJECT, TINYINT] to the '-' operator");
        assertParsingError("object - short1", "Cannot apply [OBJECT, SMALLINT] to the '-' operator");
        assertParsingError("object - int1", "Cannot apply [OBJECT, INTEGER] to the '-' operator");
        assertParsingError("object - long1", "Cannot apply [OBJECT, BIGINT] to the '-' operator");

        assertParsingError("object - float1", "Cannot apply [OBJECT, REAL] to the '-' operator");
        assertParsingError("object - double1", "Cannot apply [OBJECT, DOUBLE] to the '-' operator");

        assertParsingError("object - decimal1", "Cannot apply [OBJECT, DECIMAL] to the '-' operator");
        assertParsingError("object - bigInteger1", "Cannot apply [OBJECT, DECIMAL] to the '-' operator");

        assertParsingError("object - string1", "Cannot apply [OBJECT, VARCHAR] to the '-' operator");
        assertParsingError("object - char1", "Cannot apply [OBJECT, VARCHAR] to the '-' operator");

        assertParsingError("object - dateCol", "Cannot apply [OBJECT, DATE] to the '-' operator");
        assertParsingError("object - timeCol", "Cannot apply [OBJECT, TIME] to the '-' operator");
        assertParsingError("object - dateTimeCol", "Cannot apply [OBJECT, TIMESTAMP] to the '-' operator");
        assertParsingError("object - offsetDateTimeCol", "Cannot apply [OBJECT, TIMESTAMP_WITH_TIME_ZONE] to the '-' operator");

        assertParsingError("object - object", "Cannot apply [OBJECT, OBJECT] to the '-' operator");
    }
}
