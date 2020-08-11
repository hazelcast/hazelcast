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

import com.hazelcast.sql.impl.expression.math.ExpressionMath;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INT;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CastEndToEndTest extends ExpressionEndToEndTestBase {

    @Test
    public void testBoolean() {
        assertRow("cast(booleanTrue as boolean)", EXPR0, BOOLEAN, true);

        assertParsingError("cast(byte1 as boolean)", "Cast function cannot convert value of type TINYINT to type BOOLEAN");
        assertParsingError("cast(short1 as boolean)", "Cast function cannot convert value of type SMALLINT to type BOOLEAN");
        assertParsingError("cast(int1 as boolean)", "Cast function cannot convert value of type INTEGER to type BOOLEAN");
        assertParsingError("cast(long1 as boolean)", "Cast function cannot convert value of type BIGINT to type BOOLEAN");

        assertParsingError("cast(float1 as boolean)", "Cast function cannot convert value of type REAL to type BOOLEAN");
        assertParsingError("cast(double1 as boolean)", "Cast function cannot convert value of type DOUBLE to type BOOLEAN");

        assertParsingError("cast(decimal1 as boolean)",
                "Cast function cannot convert value of type DECIMAL(38, 38) to type BOOLEAN");
        assertParsingError("cast(bigInteger1 as boolean)",
                "Cast function cannot convert value of type DECIMAL(38, 38) to type BOOLEAN");

        assertDataError("cast(string1 as boolean)", "VARCHAR value cannot be converted to BOOLEAN: 1");
        assertRow("cast(stringFalse as boolean)", EXPR0, BOOLEAN, false);
        assertDataError("cast(char1 as boolean)", "VARCHAR value cannot be converted to BOOLEAN: 1");

        assertDataError("cast(object as boolean)", "Cannot convert OBJECT to BOOLEAN");
    }

    @Test
    public void testTinyint() {
        assertParsingError("cast(booleanTrue as tinyint)", "Cast function cannot convert value of type BOOLEAN to type TINYINT");

        assertRow("cast(byte1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertRow("cast(byteMax as tinyint)", EXPR0, TINYINT, Byte.MAX_VALUE);
        assertRow("cast(short1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(shortMax as tinyint)", "Numeric overflow while converting SMALLINT to TINYINT: 32767");
        assertRow("cast(int1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(intMax as tinyint)", "Numeric overflow while converting INT to TINYINT: 2147483647");
        assertRow("cast(long1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(longMax as tinyint)", "Numeric overflow while converting BIGINT to TINYINT: 9223372036854775807");

        assertRow("cast(float1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(floatMax as tinyint)", "Numeric overflow while converting REAL to TINYINT: 3.4028235E38");
        assertRow("cast(double1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(doubleMax as tinyint)",
                "Numeric overflow while converting DOUBLE to TINYINT: 1.7976931348623157E308");

        assertRow("cast(decimal1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(decimalBig as tinyint)",
                "Numeric overflow while converting DECIMAL to TINYINT: 92233720368547758070");
        assertRow("cast(bigInteger1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(bigIntegerBig as tinyint)",
                "Numeric overflow while converting DECIMAL to TINYINT: 92233720368547758070");

        assertRow("cast(string1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(stringBig as tinyint)", "Cannot convert VARCHAR to TINYINT: 92233720368547758070");
        assertDataError("cast(stringFoo as tinyint)", "Cannot convert VARCHAR to TINYINT: foo");
        assertRow("cast(char1 as tinyint)", EXPR0, TINYINT, (byte) 1);
        assertDataError("cast(charF as tinyint)", "Cannot convert VARCHAR to TINYINT: f");

        assertDataError("cast(object as tinyint)", "Cannot convert OBJECT to TINYINT");
    }

    @Test
    public void testSmallint() {
        assertParsingError("cast(booleanTrue as smallint)",
                "Cast function cannot convert value of type BOOLEAN to type SMALLINT");

        assertRow("cast(byte1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertRow("cast(byteMax as smallint)", EXPR0, SMALLINT, (short) Byte.MAX_VALUE);
        assertRow("cast(short1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertRow("cast(shortMax as smallint)", EXPR0, SMALLINT, Short.MAX_VALUE);
        assertRow("cast(int1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertDataError("cast(intMax as smallint)", "Numeric overflow while converting INT to SMALLINT: 2147483647");
        assertRow("cast(long1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertDataError("cast(longMax as smallint)", "Numeric overflow while converting BIGINT to SMALLINT: 9223372036854775807");

        assertRow("cast(float1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertDataError("cast(floatMax as smallint)", "Numeric overflow while converting REAL to SMALLINT: 3.4028235E38");
        assertRow("cast(double1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertDataError("cast(doubleMax as smallint)",
                "Numeric overflow while converting DOUBLE to SMALLINT: 1.7976931348623157E308");

        assertRow("cast(decimal1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertDataError("cast(decimalBig as smallint)",
                "Numeric overflow while converting DECIMAL to SMALLINT: 92233720368547758070");
        assertRow("cast(bigInteger1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertDataError("cast(bigIntegerBig as smallint)",
                "Numeric overflow while converting DECIMAL to SMALLINT: 92233720368547758070");

        assertRow("cast(string1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertDataError("cast(stringBig as smallint)", "Cannot convert VARCHAR to SMALLINT: 92233720368547758070");
        assertDataError("cast(stringFoo as smallint)", "Cannot convert VARCHAR to SMALLINT: foo");
        assertRow("cast(char1 as smallint)", EXPR0, SMALLINT, (short) 1);
        assertDataError("cast(charF as smallint)", "Cannot convert VARCHAR to SMALLINT: f");

        assertDataError("cast(object as smallint)", "Cannot convert OBJECT to SMALLINT");
    }

    @Test
    public void testInteger() {
        assertParsingError("cast(booleanTrue as integer)", "Cast function cannot convert value of type BOOLEAN to type INTEGER");

        assertRow("cast(byte1 as integer)", EXPR0, INT, 1);
        assertRow("cast(byteMax as integer)", EXPR0, INT, (int) Byte.MAX_VALUE);
        assertRow("cast(short1 as integer)", EXPR0, INT, 1);
        assertRow("cast(shortMax as integer)", EXPR0, INT, (int) Short.MAX_VALUE);
        assertRow("cast(int1 as integer)", EXPR0, INT, 1);
        assertRow("cast(intMax as integer)", EXPR0, INT, Integer.MAX_VALUE);
        assertRow("cast(long1 as integer)", EXPR0, INT, 1);
        assertDataError("cast(longMax as integer)", "Numeric overflow while converting BIGINT to INT: 9223372036854775807");

        assertRow("cast(float1 as integer)", EXPR0, INT, 1);
        assertDataError("cast(floatMax as integer)", "Numeric overflow while converting REAL to INT: 3.4028235E38");
        assertRow("cast(double1 as integer)", EXPR0, INT, 1);
        assertDataError("cast(doubleMax as integer)", "Numeric overflow while converting DOUBLE to INT: 1.7976931348623157E308");

        assertRow("cast(decimal1 as integer)", EXPR0, INT, 1);
        assertDataError("cast(decimalBig as integer)", "Numeric overflow while converting DECIMAL to INT: 92233720368547758070");
        assertRow("cast(bigInteger1 as integer)", EXPR0, INT, 1);
        assertDataError("cast(bigIntegerBig as integer)",
                "Numeric overflow while converting DECIMAL to INT: 92233720368547758070");

        assertRow("cast(string1 as integer)", EXPR0, INT, 1);
        assertDataError("cast(stringBig as integer)", "Cannot convert VARCHAR to INT: 92233720368547758070");
        assertDataError("cast(stringFoo as integer)", "Cannot convert VARCHAR to INT: foo");
        assertRow("cast(char1 as integer)", EXPR0, INT, 1);
        assertDataError("cast(charF as integer)", "Cannot convert VARCHAR to INT: f");

        assertDataError("cast(object as integer)", "Cannot convert OBJECT to INT");
    }

    @Test
    public void testBigint() {
        assertParsingError("cast(booleanTrue as bigint)", "Cast function cannot convert value of type BOOLEAN to type BIGINT");

        assertRow("cast(byte1 as bigint)", EXPR0, BIGINT, 1L);
        assertRow("cast(byteMax as bigint)", EXPR0, BIGINT, (long) Byte.MAX_VALUE);
        assertRow("cast(short1 as bigint)", EXPR0, BIGINT, 1L);
        assertRow("cast(shortMax as bigint)", EXPR0, BIGINT, (long) Short.MAX_VALUE);
        assertRow("cast(int1 as bigint)", EXPR0, BIGINT, 1L);
        assertRow("cast(intMax as bigint)", EXPR0, BIGINT, (long) Integer.MAX_VALUE);
        assertRow("cast(long1 as bigint)", EXPR0, BIGINT, 1L);
        assertRow("cast(longMax as bigint)", EXPR0, BIGINT, Long.MAX_VALUE);

        assertRow("cast(float1 as bigint)", EXPR0, BIGINT, 1L);
        assertDataError("cast(floatMax as bigint)", "Numeric overflow while converting REAL to BIGINT: 3.4028235E38");
        assertRow("cast(double1 as bigint)", EXPR0, BIGINT, 1L);
        assertDataError("cast(doubleMax as bigint)",
                "Numeric overflow while converting DOUBLE to BIGINT: 1.7976931348623157E308");

        assertRow("cast(decimal1 as bigint)", EXPR0, BIGINT, 1L);
        assertDataError("cast(decimalBig as bigint)",
                "Numeric overflow while converting DECIMAL to BIGINT: 92233720368547758070");
        assertRow("cast(bigInteger1 as bigint)", EXPR0, BIGINT, 1L);
        assertDataError("cast(bigIntegerBig as bigint)",
                "Numeric overflow while converting DECIMAL to BIGINT: 92233720368547758070");

        assertRow("cast(string1 as bigint)", EXPR0, BIGINT, 1L);
        assertDataError("cast(stringBig as bigint)", "Cannot convert VARCHAR to BIGINT: 92233720368547758070");
        assertDataError("cast(stringFoo as bigint)", "Cannot convert VARCHAR to BIGINT: foo");
        assertRow("cast(char1 as bigint)", EXPR0, BIGINT, 1L);
        assertDataError("cast(charF as bigint)", "Cannot convert VARCHAR to BIGINT: f");

        assertDataError("cast(object as bigint)", "Cannot convert OBJECT to BIGINT");
    }

    @Test
    public void testReal() {
        assertParsingError("cast(booleanTrue as real)", "Cast function cannot convert value of type BOOLEAN to type REAL");

        assertRow("cast(byte1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(byteMax as real)", EXPR0, REAL, (float) Byte.MAX_VALUE);
        assertRow("cast(short1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(shortMax as real)", EXPR0, REAL, (float) Short.MAX_VALUE);
        assertRow("cast(int1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(intMax as real)", EXPR0, REAL, (float) Integer.MAX_VALUE);
        assertRow("cast(long1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(longMax as real)", EXPR0, REAL, (float) Long.MAX_VALUE);

        assertRow("cast(float1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(floatMax as real)", EXPR0, REAL, Float.MAX_VALUE);
        assertRow("cast(double1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(doubleMax as real)", EXPR0, REAL, (float) Double.MAX_VALUE);

        assertRow("cast(decimal1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(decimalBig as real)", EXPR0, REAL, getRecord().decimalBig.floatValue());
        assertRow("cast(bigInteger1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(bigIntegerBig as real)", EXPR0, REAL, getRecord().bigIntegerBig.floatValue());

        assertRow("cast(string1 as real)", EXPR0, REAL, 1.0f);
        assertRow("cast(stringBig as real)", EXPR0, REAL, Float.parseFloat(getRecord().stringBig));
        assertDataError("cast(stringFoo as real)", "Cannot convert VARCHAR to REAL: foo");
        assertRow("cast(char1 as real)", EXPR0, REAL, 1.0f);
        assertDataError("cast(charF as real)", "Cannot convert VARCHAR to REAL: f");

        assertDataError("cast(object as real)", "Cannot convert OBJECT to REAL");
    }

    @Test
    public void testDouble() {
        assertParsingError("cast(booleanTrue as double)", "Cast function cannot convert value of type BOOLEAN to type DOUBLE");

        assertRow("cast(byte1 as double)", EXPR0, DOUBLE, 1.0);
        assertRow("cast(byteMax as double)", EXPR0, DOUBLE, (double) Byte.MAX_VALUE);
        assertRow("cast(short1 as double)", EXPR0, DOUBLE, 1.0d);
        assertRow("cast(shortMax as double)", EXPR0, DOUBLE, (double) Short.MAX_VALUE);
        assertRow("cast(int1 as double)", EXPR0, DOUBLE, 1.0d);
        assertRow("cast(intMax as double)", EXPR0, DOUBLE, (double) Integer.MAX_VALUE);
        assertRow("cast(long1 as double)", EXPR0, DOUBLE, 1.0d);
        assertRow("cast(longMax as double)", EXPR0, DOUBLE, (double) Long.MAX_VALUE);

        assertRow("cast(float1 as double)", EXPR0, DOUBLE, 1.0d);
        assertRow("cast(floatMax as double)", EXPR0, DOUBLE, (double) Float.MAX_VALUE);
        assertRow("cast(double1 as double)", EXPR0, DOUBLE, 1.0d);
        assertRow("cast(doubleMax as double)", EXPR0, DOUBLE, Double.MAX_VALUE);

        assertRow("cast(decimal1 as double)", EXPR0, DOUBLE, 1.0d);
        assertRow("cast(decimalBig as double)", EXPR0, DOUBLE, getRecord().decimalBig.doubleValue());
        assertRow("cast(bigInteger1 as double)", EXPR0, DOUBLE, 1.0d);
        assertRow("cast(bigIntegerBig as double)", EXPR0, DOUBLE, getRecord().bigIntegerBig.doubleValue());

        assertRow("cast(string1 as double)", EXPR0, DOUBLE, 1.0d);
        assertRow("cast(stringBig as double)", EXPR0, DOUBLE, Double.parseDouble(getRecord().stringBig));
        assertDataError("cast(stringFoo as double)", "Cannot convert VARCHAR to DOUBLE: foo");
        assertRow("cast(char1 as double)", EXPR0, DOUBLE, 1.0d);
        assertDataError("cast(charF as double)", "Cannot convert VARCHAR to DOUBLE: f");

        assertDataError("cast(object as double)", "Cannot convert OBJECT to DOUBLE");
    }

    @Test
    public void testDecimal() {
        assertParsingError("cast(booleanTrue as decimal)", "Cast function cannot convert value of type BOOLEAN to type DECIMAL");

        assertRow("cast(byte1 as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("cast(byteMax as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(Byte.MAX_VALUE));
        assertRow("cast(short1 as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("cast(shortMax as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(Short.MAX_VALUE));
        assertRow("cast(int1 as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("cast(intMax as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(Integer.MAX_VALUE));
        assertRow("cast(long1 as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("cast(longMax as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(Long.MAX_VALUE));

        assertRow("cast(float1 as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("cast(floatMax as decimal)", EXPR0, DECIMAL,
                new BigDecimal(Float.MAX_VALUE, ExpressionMath.DECIMAL_MATH_CONTEXT));
        assertRow("cast(double1 as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("cast(doubleMax as decimal)", EXPR0, DECIMAL,
                new BigDecimal(Double.MAX_VALUE, ExpressionMath.DECIMAL_MATH_CONTEXT));

        assertRow("cast(decimal1 as decimal)", EXPR0, DECIMAL, getRecord().decimal1);
        assertRow("cast(decimalBig as decimal)", EXPR0, DECIMAL, getRecord().decimalBig);
        assertRow("cast(bigInteger1 as decimal)", EXPR0, DECIMAL, getRecord().decimal1);
        assertRow("cast(bigIntegerBig as decimal)", EXPR0, DECIMAL, getRecord().decimalBig);

        assertRow("cast(string1 as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertRow("cast(stringBig as decimal)", EXPR0, DECIMAL, new BigDecimal(getRecord().stringBig));
        assertDataError("cast(stringFoo as decimal)", "Cannot convert VARCHAR to DECIMAL: foo");
        assertRow("cast(char1 as decimal)", EXPR0, DECIMAL, BigDecimal.valueOf(1));
        assertDataError("cast(charF as decimal)", "Cannot convert VARCHAR to DECIMAL: f");

        assertDataError("cast(object as decimal)", "Cannot convert OBJECT to DECIMAL");
    }

    @Test
    public void testVarchar() {
        assertRow("cast(booleanTrue as varchar)", EXPR0, VARCHAR, "true");

        assertRow("cast(byte1 as varchar)", EXPR0, VARCHAR, "1");
        assertRow("cast(byteMax as varchar)", EXPR0, VARCHAR, Byte.toString(Byte.MAX_VALUE));
        assertRow("cast(short1 as varchar)", EXPR0, VARCHAR, "1");
        assertRow("cast(shortMax as varchar)", EXPR0, VARCHAR, Short.toString(Short.MAX_VALUE));
        assertRow("cast(int1 as varchar)", EXPR0, VARCHAR, "1");
        assertRow("cast(intMax as varchar)", EXPR0, VARCHAR, Integer.toString(Integer.MAX_VALUE));
        assertRow("cast(long1 as varchar)", EXPR0, VARCHAR, "1");
        assertRow("cast(longMax as varchar)", EXPR0, VARCHAR, Long.toString(Long.MAX_VALUE));

        assertRow("cast(float1 as varchar)", EXPR0, VARCHAR, "1.0");
        assertRow("cast(floatMax as varchar)", EXPR0, VARCHAR, Float.toString(Float.MAX_VALUE));
        assertRow("cast(double1 as varchar)", EXPR0, VARCHAR, "1.0");
        assertRow("cast(doubleMax as varchar)", EXPR0, VARCHAR, Double.toString(Double.MAX_VALUE));

        assertRow("cast(decimal1 as varchar)", EXPR0, VARCHAR, "1");
        assertRow("cast(decimalBig as varchar)", EXPR0, VARCHAR, getRecord().decimalBig.toString());
        assertRow("cast(bigInteger1 as varchar)", EXPR0, VARCHAR, "1");
        assertRow("cast(bigIntegerBig as varchar)", EXPR0, VARCHAR, getRecord().decimalBig.toString());

        assertRow("cast(string1 as varchar)", EXPR0, VARCHAR, "1");
        assertRow("cast(stringBig as varchar)", EXPR0, VARCHAR, getRecord().stringBig);
        assertRow("cast(stringFoo as varchar)", EXPR0, VARCHAR, getRecord().stringFoo);
        assertRow("cast(char1 as varchar)", EXPR0, VARCHAR, "1");
        assertRow("cast(charF as varchar)", EXPR0, VARCHAR, Character.toString(getRecord().charF));

        assertRow("cast(object as varchar)", EXPR0, VARCHAR, getRecord().object.toString());
    }

    @Test
    public void testObject() {
        assertRow("cast(booleanTrue as object)", EXPR0, OBJECT, getRecord().booleanTrue);

        assertRow("cast(byte1 as object)", EXPR0, OBJECT, getRecord().byte1);
        assertRow("cast(byteMax as object)", EXPR0, OBJECT, getRecord().byteMax);
        assertRow("cast(short1 as object)", EXPR0, OBJECT, getRecord().short1);
        assertRow("cast(shortMax as object)", EXPR0, OBJECT, getRecord().shortMax);
        assertRow("cast(int1 as object)", EXPR0, OBJECT, getRecord().int1);
        assertRow("cast(intMax as object)", EXPR0, OBJECT, getRecord().intMax);
        assertRow("cast(long1 as object)", EXPR0, OBJECT, getRecord().long1);
        assertRow("cast(longMax as object)", EXPR0, OBJECT, getRecord().longMax);

        assertRow("cast(float1 as object)", EXPR0, OBJECT, getRecord().float1);
        assertRow("cast(floatMax as object)", EXPR0, OBJECT, getRecord().floatMax);
        assertRow("cast(double1 as object)", EXPR0, OBJECT, getRecord().double1);
        assertRow("cast(doubleMax as object)", EXPR0, OBJECT, getRecord().doubleMax);

        assertRow("cast(decimal1 as object)", EXPR0, OBJECT, getRecord().decimal1);
        assertRow("cast(decimalBig as object)", EXPR0, OBJECT, getRecord().decimalBig);
        assertRow("cast(bigInteger1 as object)", EXPR0, OBJECT, getRecord().decimal1);
        assertRow("cast(bigIntegerBig as object)", EXPR0, OBJECT, getRecord().decimalBig);

        assertRow("cast(string1 as object)", EXPR0, OBJECT, getRecord().string1);
        assertRow("cast(stringBig as object)", EXPR0, OBJECT, getRecord().stringBig);
        assertRow("cast(stringFoo as object)", EXPR0, OBJECT, getRecord().stringFoo);
        assertRow("cast(char1 as object)", EXPR0, OBJECT, getRecord().string1);
        assertRow("cast(charF as object)", EXPR0, OBJECT, Character.toString(getRecord().charF));

        assertRow("cast(object as object)", EXPR0, OBJECT, getRecord().object);
    }

}
