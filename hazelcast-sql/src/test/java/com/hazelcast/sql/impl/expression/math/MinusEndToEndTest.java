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
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MinusEndToEndTest extends ExpressionEndToEndTestBase {

    @Test
    public void testBoolean() {
        assertParsingError("booleanTrue - booleanTrue", "Cannot apply '-' to arguments of type '<BOOLEAN> - <BOOLEAN>'");

        assertParsingError("booleanTrue - byte1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <TINYINT>'");
        assertParsingError("booleanTrue - short1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <SMALLINT>'");
        assertParsingError("booleanTrue - int1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <INTEGER>'");
        assertParsingError("booleanTrue - long1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <BIGINT>'");

        assertParsingError("booleanTrue - float1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <REAL>'");
        assertParsingError("booleanTrue - double1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <DOUBLE>'");

        assertParsingError("booleanTrue - decimal1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <DECIMAL(38, 38)>'");
        assertParsingError("booleanTrue - bigInteger1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <DECIMAL(38, 38)>'");

        assertParsingError("booleanTrue - string1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <BOOLEAN>'");
        assertParsingError("booleanTrue - char1", "Cannot apply '-' to arguments of type '<BOOLEAN> - <BOOLEAN>'");

        assertParsingError("booleanTrue - object", "Cannot apply '-' to arguments of type '<BOOLEAN> - <OBJECT>'");
    }

    @Test
    public void testTinyint() {
        assertParsingError("byte1 - booleanTrue", "Cannot apply '-' to arguments of type '<TINYINT> - <BOOLEAN>'");

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

        assertRow("byte1 - string1", EXPR0, BIGINT, 0L);
        assertDataError("byte1 - stringBigNegative", "Cannot convert VARCHAR to BIGINT");
        assertDataError("byte1 - stringFoo", "Cannot convert VARCHAR to BIGINT");
        assertRow("byte1 - char1", EXPR0, BIGINT, 0L);
        assertDataError("byte1 - charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("byte1 - object", "Cannot apply '-' to arguments of type '<TINYINT> - <OBJECT>'");
    }

    @Test
    public void testSmallint() {
        assertParsingError("short1 - booleanTrue", "Cannot apply '-' to arguments of type '<SMALLINT> - <BOOLEAN>'");

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

        assertRow("short1 - string1", EXPR0, BIGINT, 0L);
        assertDataError("short1 - stringBigNegative", "Cannot convert VARCHAR to BIGINT");
        assertDataError("short1 - stringFoo", "Cannot convert VARCHAR to BIGINT");
        assertRow("short1 - char1", EXPR0, BIGINT, 0L);
        assertDataError("short1 - charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("short1 - object", "Cannot apply '-' to arguments of type '<SMALLINT> - <OBJECT>'");
    }

    @Test
    public void testInteger() {
        assertParsingError("int1 - booleanTrue", "Cannot apply '-' to arguments of type '<INTEGER> - <BOOLEAN>'");

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

        assertRow("int1 - string1", EXPR0, BIGINT, 0L);
        assertDataError("int1 - stringBigNegative", "Cannot convert VARCHAR to BIGINT");
        assertDataError("int1 - stringFoo", "Cannot convert VARCHAR to BIGINT");
        assertRow("int1 - char1", EXPR0, BIGINT, 0L);
        assertDataError("int1 - charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("int1 - object", "Cannot apply '-' to arguments of type '<INTEGER> - <OBJECT>'");
    }

    @Test
    public void testBigint() {
        assertParsingError("long1 - booleanTrue", "Cannot apply '-' to arguments of type '<BIGINT> - <BOOLEAN>'");

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

        assertRow("long1 - string1", EXPR0, BIGINT, 0L);
        assertDataError("long1 - stringBigNegative", "Cannot convert VARCHAR to BIGINT");
        assertDataError("long1 - stringFoo", "Cannot convert VARCHAR to BIGINT");
        assertRow("long1 - char1", EXPR0, BIGINT, 0L);
        assertDataError("long1 - charF", "Cannot convert VARCHAR to BIGINT");

        assertParsingError("long1 - object", "Cannot apply '-' to arguments of type '<BIGINT> - <OBJECT>'");
    }

    @Test
    public void testReal() {
        assertParsingError("float1 - booleanTrue", "Cannot apply '-' to arguments of type '<REAL> - <BOOLEAN>'");

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

        assertRow("float1 - string1", EXPR0, REAL, 0.0f);
        assertRow("float1 - stringBigNegative", EXPR0, REAL, 1.0f - Float.parseFloat(getRecord().stringBigNegative));
        assertDataError("float1 - stringFoo", "Cannot convert VARCHAR to REAL");
        assertRow("float1 - char1", EXPR0, REAL, 0.0f);
        assertDataError("float1 - charF", "Cannot convert VARCHAR to REAL");

        assertParsingError("float1 - object", "Cannot apply '-' to arguments of type '<REAL> - <OBJECT>'");
    }

    @Test
    public void testDouble() {
        assertParsingError("double1 - booleanTrue", "Cannot apply '-' to arguments of type '<DOUBLE> - <BOOLEAN>'");

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

        assertRow("double1 - string1", EXPR0, DOUBLE, 0.0);
        assertRow("double1 - stringBigNegative", EXPR0, DOUBLE, 1.0 - Double.parseDouble(getRecord().stringBigNegative));
        assertDataError("double1 - stringFoo", "Cannot convert VARCHAR to DOUBLE");
        assertRow("double1 - char1", EXPR0, DOUBLE, 0.0);
        assertDataError("double1 - charF", "Cannot convert VARCHAR to DOUBLE");

        assertParsingError("double1 - object", "Cannot apply '-' to arguments of type '<DOUBLE> - <OBJECT>'");
    }

    @Test
    public void testDecimal() {
        assertParsingError("decimal1 - booleanTrue", "Cannot apply '-' to arguments of type '<DECIMAL(38, 38)> - <BOOLEAN>'");

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

        assertRow("decimal1 - string1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("decimal1 - stringBigNegative", EXPR0, DECIMAL,
                getRecord().decimal1.subtract(new BigDecimal(getRecord().stringBigNegative)));
        assertDataError("decimal1 - stringFoo", "Cannot convert VARCHAR to DECIMAL");
        assertRow("decimal1 - char1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertDataError("decimal1 - charF", "Cannot convert VARCHAR to DECIMAL");

        assertParsingError("decimal1 - object", "Cannot apply '-' to arguments of type '<DECIMAL(38, 38)> - <OBJECT>'");
    }

    @Test
    public void testVarchar() {
        assertParsingError("string1 - booleanTrue", "Cannot apply '-' to arguments of type '<BOOLEAN> - <BOOLEAN>'");

        assertRow("string1 - byte1", EXPR0, BIGINT, 0L);
        assertRow("string1 - byteMin", EXPR0, BIGINT, 1L - Byte.MIN_VALUE);
        assertRow("string1 - short1", EXPR0, BIGINT, 0L);
        assertRow("string1 - shortMin", EXPR0, BIGINT, 1L - Short.MIN_VALUE);
        assertRow("string1 - int1", EXPR0, BIGINT, 0L);
        assertRow("string1 - intMin", EXPR0, BIGINT, 1L - Integer.MIN_VALUE);
        assertRow("string1 - long1", EXPR0, BIGINT, 0L);
        assertDataError("string1 - longMin", "BIGINT overflow");

        assertRow("string1 - float1", EXPR0, REAL, 0.0f);
        assertRow("string1 - floatMin", EXPR0, REAL, 1 - Float.MIN_VALUE);
        assertRow("string1 - double1", EXPR0, DOUBLE, 0.0);
        assertRow("string1 - doubleMin", EXPR0, DOUBLE, 1 - Double.MIN_VALUE);

        assertRow("string1 - decimal1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("string1 - decimalBigNegative", EXPR0, DECIMAL, BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));
        assertRow("string1 - bigInteger1", EXPR0, DECIMAL, BigDecimal.valueOf(0));
        assertRow("string1 - bigIntegerBigNegative", EXPR0, DECIMAL,
                BigDecimal.valueOf(1).subtract(getRecord().decimalBigNegative));

        assertRow("string1 - string1", EXPR0, DOUBLE, 0.0);
        assertRow("string1 - stringBigNegative", EXPR0, DOUBLE, 1.0 - Double.parseDouble(getRecord().stringBigNegative));
        assertDataError("string1 - stringFoo", "Cannot convert VARCHAR to DOUBLE");
        assertRow("string1 - char1", EXPR0, DOUBLE, 0.0);
        assertDataError("string1 - charF", "Cannot convert VARCHAR to DOUBLE");

        assertParsingError("string1 - object", "Cannot apply '-' to arguments of type '<VARCHAR> - <OBJECT>'");
    }

    @Test
    public void testObject() {
        assertParsingError("object - booleanTrue", "Cannot apply '-' to arguments of type '<OBJECT> - <BOOLEAN>'");

        assertParsingError("object - byte1", "Cannot apply '-' to arguments of type '<OBJECT> - <TINYINT>'");
        assertParsingError("object - short1", "Cannot apply '-' to arguments of type '<OBJECT> - <SMALLINT>'");
        assertParsingError("object - int1", "Cannot apply '-' to arguments of type '<OBJECT> - <INTEGER>'");
        assertParsingError("object - long1", "Cannot apply '-' to arguments of type '<OBJECT> - <BIGINT>'");

        assertParsingError("object - float1", "Cannot apply '-' to arguments of type '<OBJECT> - <REAL>'");
        assertParsingError("object - double1", "Cannot apply '-' to arguments of type '<OBJECT> - <DOUBLE>'");

        assertParsingError("object - decimal1", "Cannot apply '-' to arguments of type '<OBJECT> - <DECIMAL(38, 38)>'");
        assertParsingError("object - bigInteger1", "Cannot apply '-' to arguments of type '<OBJECT> - <DECIMAL(38, 38)>'");

        assertParsingError("object - string1", "Cannot apply '-' to arguments of type '<OBJECT> - <VARCHAR>'");
        assertParsingError("object - char1", "Cannot apply '-' to arguments of type '<OBJECT> - <VARCHAR>'");

        assertParsingError("object - object", "Cannot apply '-' to arguments of type '<OBJECT> - <OBJECT>'");
    }

}
