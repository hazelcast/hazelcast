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

import com.hazelcast.sql.SqlErrorCode;
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
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ParameterEndToEndTest extends ExpressionEndToEndTestBase {

    @Test
    public void testBoolean() {
        assertRow("booleanTrue and ?", BOOLEAN, false, false);
        assertRow("booleanTrue and ?", BOOLEAN, true, true);
        assertRow("booleanTrue and ?", BOOLEAN, true, "tRuE");
        assertRow("booleanTrue and ? and ?", BOOLEAN, false, "tRuE", false);
        assertRow("? and ? and ?", BOOLEAN, true, "tRuE", true, "true");
        assertDataError("booleanTrue and ?", "failed to convert parameter", "foo");
        assertDataError("booleanTrue and ?", "failed to convert parameter", 1);
    }

    @Test
    public void testByte() {
        assertRow("byte1 + ?", BIGINT, 1L, 0);
        assertRow("byte1 + ?", BIGINT, 2L, 1.0d);
        assertRow("byte1 + ?", BIGINT, 2L, 1.1d);
        assertRow("byte1 + ?", BIGINT, 2L, "1");
        assertDataError("byte1 + ?", "failed to convert parameter", "1.1");
        assertDataError("byte1 + ?", "failed to convert parameter", "foo");
    }

    @Test
    public void testShort() {
        assertRow("short1 + ?", BIGINT, 1L, 0);
        assertRow("short1 + ?", BIGINT, 2L, 1.0d);
        assertRow("short1 + ?", BIGINT, 2L, 1.1d);
        assertRow("short1 + ?", BIGINT, 2L, "1");
        assertDataError("short1 + ?", "failed to convert parameter", "1.1");
        assertDataError("short1 + ?", "failed to convert parameter", "foo");
    }

    @Test
    public void testInt() {
        assertRow("int1 + ?", BIGINT, 1L, 0);
        assertRow("int1 + ?", BIGINT, 2L, 1.0d);
        assertRow("int1 + ?", BIGINT, 2L, 1.1d);
        assertRow("int1 + ?", BIGINT, 2L, "1");
        assertDataError("int1 + ?", "failed to convert parameter", "1.1");
        assertDataError("int1 + ?", "failed to convert parameter", "foo");
    }

    @Test
    public void testLong() {
        assertRow("long1 + ?", BIGINT, 1L, 0);
        assertRow("long1 + ?", BIGINT, 2L, 1.0d);
        assertRow("long1 + ?", BIGINT, 2L, 1.1d);
        assertRow("long1 + ?", BIGINT, 2L, "1");
        assertDataError("long1 + ?", "failed to convert parameter", "1.1");
        assertDataError("long1 + ?", "failed to convert parameter", "foo");
    }

    @Test
    public void testFloat() {
        assertRow("float1 + ?", REAL, 1.0f, 0);
        assertRow("float1 + ?", REAL, 2.0f, 1d);
        assertRow("float1 + ?", REAL, 2.1f, 1.1d);
        assertRow("float1 + ?", REAL, 2.0f, "1");
        assertRow("float1 + ?", REAL, 2.1f, "1.1");
        assertDataError("float1 + ?", "failed to convert parameter", "foo");
    }

    @Test
    public void testDouble() {
        assertRow("double1 + ?", DOUBLE, 1.0d, 0);
        assertRow("double1 + ?", DOUBLE, 2.0d, 1d);
        assertRow("double1 + ?", DOUBLE, 2.1d, 1.1d);
        assertRow("double1 + ?", DOUBLE, 2.0d, "1");
        assertRow("double1 + ?", DOUBLE, 2.1d, "1.1");
        assertDataError("double1 + ?", "failed to convert parameter", "foo");
    }

    @SuppressWarnings("UnpredictableBigDecimalConstructorCall")
    @Test
    public void testDecimal() {
        assertRow("decimal1 + ?", DECIMAL, BigDecimal.valueOf(1), 0);
        assertRow("decimal1 + ?", DECIMAL, BigDecimal.valueOf(2), 1d);
        assertRow("decimal1 + ?", DECIMAL, new BigDecimal(1, ExpressionMath.DECIMAL_MATH_CONTEXT).add(
                new BigDecimal(1.1, ExpressionMath.DECIMAL_MATH_CONTEXT)), 1.1d);
        assertRow("decimal1 + ?", DECIMAL, BigDecimal.valueOf(2), "1");
        assertRow("decimal1 + ?", DECIMAL, BigDecimal.valueOf(2.1), "1.1");
        assertRow("decimal1 + ?", DECIMAL, BigDecimal.valueOf(2.1), BigDecimal.valueOf(1.1));
        assertDataError("decimal1 + ?", "failed to convert parameter", "foo");
    }

    @SuppressWarnings("UnpredictableBigDecimalConstructorCall")
    @Test
    public void testBigInteger() {
        assertRow("bigInteger1 + ?", DECIMAL, BigDecimal.valueOf(1), 0);
        assertRow("bigInteger1 + ?", DECIMAL, BigDecimal.valueOf(2), 1d);
        assertRow("bigInteger1 + ?", DECIMAL, new BigDecimal(1, ExpressionMath.DECIMAL_MATH_CONTEXT).add(
                new BigDecimal(1.1, ExpressionMath.DECIMAL_MATH_CONTEXT)), 1.1d);
        assertRow("bigInteger1 + ?", DECIMAL, BigDecimal.valueOf(2), "1");
        assertRow("bigInteger1 + ?", DECIMAL, BigDecimal.valueOf(2.1), "1.1");
        assertRow("bigInteger1 + ?", DECIMAL, BigDecimal.valueOf(2.1), BigDecimal.valueOf(1.1));
        assertDataError("bigInteger1 + ?", "failed to convert parameter", "foo");
    }

    @Test
    public void testString() {
        assertRow("string1 + ?", DOUBLE, 1.0d, 0);
        assertRow("string1 + ?", DOUBLE, 2.0d, 1d);
        assertRow("string1 + ?", DOUBLE, 2.1d, 1.1d);
        assertRow("string1 + ?", DOUBLE, 2.0d, "1");
        assertRow("string1 + ?", DOUBLE, 2.1d, "1.1");
        assertDataError("string1 + ?", "failed to convert parameter", "foo");
    }

    @Test
    public void testChar() {
        assertRow("char1 + ?", DOUBLE, 1.0d, 0);
        assertRow("char1 + ?", DOUBLE, 2.0d, 1d);
        assertRow("char1 + ?", DOUBLE, 2.1d, 1.1d);
        assertRow("char1 + ?", DOUBLE, 2.0d, "1");
        assertRow("char1 + ?", DOUBLE, 2.1d, "1.1");
        assertDataError("char1 + ?", "failed to convert parameter", "foo");
    }

    @Test
    public void testObject() {
        assertRow("cast(? as object) is null", BOOLEAN, true, (Object) null);
        assertRow("cast(? as object) is null", BOOLEAN, false, 1);
        assertRow("cast(? as object) is null", BOOLEAN, false, new SerializableObject());
        assertError("cast(? as object) is null", SqlErrorCode.GENERIC, "failed to serialize", new Object());
    }

    @Test
    public void testVarious() {
        assertParsingError("? + ?", "illegal use of dynamic parameter");
        assertRow("? + cast(? as tinyint)", BIGINT, 3L, 1, 2);
        assertRow("cast(? as tinyint) + cast(? as tinyint)", SMALLINT, (short) 3, 1, 2);
        assertRow("? + cast(? as double)", DOUBLE, 3.0, 1, 2);
        assertDataError("? + 1", "unexpected parameter count");
        assertDataError("? and ? and ?", "unexpected parameter count", 0, 1);
    }

}
