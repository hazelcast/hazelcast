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

package com.hazelcast.sql.impl.calcite.validate.types;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimeString;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.util.Calendar;

import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.canCast;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.canConvert;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.canRepresent;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.withHigherPrecedence;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.withHigherPrecedenceForLiterals;
import static com.hazelcast.sql.impl.expression.ExpressionTestBase.TYPE_FACTORY;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.NULL;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastTypeSystemTest {

    @Test
    public void numericPrecisionAndScaleTest() {
        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.MAX_DECIMAL_PRECISION);
        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.MAX_DECIMAL_SCALE);

        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.INSTANCE.getMaxNumericPrecision());
        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.INSTANCE.getMaxNumericScale());

        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.INSTANCE.getMaxPrecision(DECIMAL));
        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, HazelcastTypeSystem.INSTANCE.getMaxScale(DECIMAL));
    }

    @Test
    public void canCastTest() {
        assertTrue(canCast(type(NULL), type(VARCHAR)));
        assertTrue(canCast(type(VARCHAR), type(BIGINT)));
        assertFalse(canCast(type(BIGINT), type(BOOLEAN)));
    }

    @Test
    public void canRepresentTest() {
        assertTrue(canRepresent(SqlLiteral.createCharString("1", ZERO), type(BIGINT)));
        assertFalse(canRepresent(SqlLiteral.createCharString("1.1", ZERO), type(BIGINT)));
        assertFalse(canRepresent(SqlLiteral.createCharString("foo", ZERO), type(DOUBLE)));

        assertTrue(canRepresent(SqlLiteral.createBoolean(true, ZERO), type(VARCHAR)));
        assertFalse(canRepresent(SqlLiteral.createBoolean(false, ZERO), type(BIGINT)));

        assertTrue(canRepresent(SqlLiteral.createExactNumeric("1", ZERO), type(VARCHAR)));
        assertTrue(canRepresent(SqlLiteral.createExactNumeric("1.1", ZERO), type(DECIMAL)));
        assertFalse(canRepresent(SqlLiteral.createExactNumeric("1.1", ZERO), type(BOOLEAN)));

        assertTrue(canRepresent(SqlLiteral.createApproxNumeric("1", ZERO), type(VARCHAR)));
        assertFalse(canRepresent(SqlLiteral.createApproxNumeric("1.1", ZERO), type(BOOLEAN)));

        assertTrue(canRepresent(SqlLiteral.createNull(ZERO), type(REAL)));

        assertTrue(canRepresent(SqlLiteral.createTime(TimeString.fromCalendarFields(Calendar.getInstance()), 1, ZERO),
                type(VARCHAR)));
        assertFalse(canRepresent(SqlLiteral.createTime(TimeString.fromCalendarFields(Calendar.getInstance()), 1, ZERO),
                type(BOOLEAN)));
    }

    @Test
    public void canConvertTest() {
        assertTrue(canConvert("1", type(VARCHAR), type(BIGINT)));
        assertFalse(canConvert("1.1", type(VARCHAR), type(BIGINT)));
        assertFalse(canConvert("foo", type(VARCHAR), type(DOUBLE)));

        assertTrue(canConvert(true, type(BOOLEAN), type(VARCHAR)));
        assertFalse(canConvert(false, type(BOOLEAN), type(BIGINT)));

        assertTrue(canConvert(1, type(INTEGER), type(VARCHAR)));
        assertTrue(canConvert(1.1, type(DOUBLE), type(DECIMAL)));
        assertFalse(canConvert(1.1, type(DECIMAL), type(BOOLEAN)));

        assertTrue(canConvert(1, type(REAL), type(VARCHAR)));
        assertFalse(canConvert(1.1, type(DOUBLE), type(BOOLEAN)));

        assertTrue(canConvert(null, type(BOOLEAN), type(VARCHAR)));
        assertTrue(canConvert(null, type(NULL), type(TINYINT)));
        assertFalse(canConvert(null, type(REAL), type(BOOLEAN)));

        assertTrue(canConvert(Calendar.getInstance(), type(TIME), type(VARCHAR)));
        assertFalse(canConvert(Calendar.getInstance(), type(TIME), type(BOOLEAN)));
    }

    @Test
    public void withHigherPrecedenceTest() {
        assertPrecedence(type(VARCHAR), type(NULL));
        assertPrecedence(type(BOOLEAN), type(VARCHAR));
        assertPrecedence(type(TINYINT), type(BOOLEAN));
        assertPrecedence(HazelcastIntegerType.of(Byte.SIZE - 1, false), HazelcastIntegerType.of(Byte.SIZE - 2, false));
        assertPrecedence(type(SMALLINT), type(TINYINT));
        assertPrecedence(HazelcastIntegerType.of(Short.SIZE - 1, false), HazelcastIntegerType.of(Short.SIZE - 2, false));
        assertPrecedence(type(INTEGER), type(SMALLINT));
        assertPrecedence(HazelcastIntegerType.of(Integer.SIZE - 1, false), HazelcastIntegerType.of(Integer.SIZE - 2, false));
        assertPrecedence(type(BIGINT), type(INTEGER));
        assertPrecedence(HazelcastIntegerType.of(Long.SIZE - 1, false), HazelcastIntegerType.of(Long.SIZE - 2, false));
        assertPrecedence(type(DECIMAL), type(BIGINT));
        assertPrecedence(type(REAL), type(DECIMAL));
        assertPrecedence(type(DOUBLE), type(REAL));
        assertPrecedence(type(INTERVAL_YEAR_MONTH), type(DOUBLE));
        assertPrecedence(type(INTERVAL_DAY_SECOND), type(INTERVAL_YEAR_MONTH));
        assertPrecedence(type(TIME), type(INTERVAL_DAY_SECOND));
        assertPrecedence(type(DATE), type(TIME));
        assertPrecedence(type(TIMESTAMP), type(DATE));
        assertPrecedence(type(TIMESTAMP_WITH_LOCAL_TIME_ZONE), type(TIMESTAMP));
        assertPrecedence(type(ANY), type(TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }

    @Test
    public void withHigherPrecedenceForLiteralsTest() {
        assertPrecedenceForLiterals(type(VARCHAR), type(NULL));
        assertPrecedenceForLiterals(type(BOOLEAN), type(VARCHAR));
        assertPrecedenceForLiterals(type(TINYINT), type(BOOLEAN));
        assertPrecedenceForLiterals(HazelcastIntegerType.of(Byte.SIZE - 1, false), HazelcastIntegerType.of(Byte.SIZE - 2, false));
        assertPrecedenceForLiterals(type(SMALLINT), type(TINYINT));
        assertPrecedenceForLiterals(HazelcastIntegerType.of(Short.SIZE - 1, false),
                HazelcastIntegerType.of(Short.SIZE - 2, false));
        assertPrecedenceForLiterals(type(INTEGER), type(SMALLINT));
        assertPrecedenceForLiterals(HazelcastIntegerType.of(Integer.SIZE - 1, false),
                HazelcastIntegerType.of(Integer.SIZE - 2, false));
        assertPrecedenceForLiterals(type(BIGINT), type(INTEGER));
        assertPrecedenceForLiterals(HazelcastIntegerType.of(Long.SIZE - 1, false), HazelcastIntegerType.of(Long.SIZE - 2, false));
        assertPrecedenceForLiterals(type(REAL), type(BIGINT));
        assertPrecedenceForLiterals(type(DOUBLE), type(REAL));
        assertPrecedenceForLiterals(type(DECIMAL), type(DOUBLE));
        assertPrecedenceForLiterals(type(INTERVAL_YEAR_MONTH), type(DECIMAL));
        assertPrecedenceForLiterals(type(INTERVAL_DAY_SECOND), type(INTERVAL_YEAR_MONTH));
        assertPrecedenceForLiterals(type(TIME), type(INTERVAL_DAY_SECOND));
        assertPrecedenceForLiterals(type(DATE), type(TIME));
        assertPrecedenceForLiterals(type(TIMESTAMP), type(DATE));
        assertPrecedenceForLiterals(type(TIMESTAMP_WITH_LOCAL_TIME_ZONE), type(TIMESTAMP));
        assertPrecedenceForLiterals(type(ANY), type(TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }

    @Test
    public void narrowestTypeForTest() {
        assertEquals(HazelcastIntegerType.of(0, false), narrowestTypeFor(BigDecimal.valueOf(0), VARCHAR));
        assertEquals(HazelcastIntegerType.of(1, false), narrowestTypeFor(BigDecimal.valueOf(1), DOUBLE));

        assertEquals(type(BIGINT), narrowestTypeFor(new BigDecimal(Long.MAX_VALUE + "0"), BOOLEAN));
        assertEquals(type(DOUBLE), narrowestTypeFor(new BigDecimal(Long.MAX_VALUE + "0"), DOUBLE));

        assertEquals(type(DOUBLE), narrowestTypeFor(BigDecimal.valueOf(0.1), TIME));
        assertEquals(type(DECIMAL), narrowestTypeFor(BigDecimal.valueOf(0.1), DECIMAL));
    }

    private static void assertPrecedence(RelDataType expected, RelDataType other) {
        RelDataType actual = withHigherPrecedence(expected, other);
        assertSame(expected, actual);
        actual = withHigherPrecedence(other, expected);
        assertSame(expected, actual);
    }

    private static void assertPrecedenceForLiterals(RelDataType expected, RelDataType other) {
        RelDataType actual = withHigherPrecedenceForLiterals(expected, other);
        assertSame(expected, actual);
        actual = withHigherPrecedenceForLiterals(other, expected);
        assertSame(expected, actual);
    }

    private static RelDataType type(SqlTypeName typeName) {
        return TYPE_FACTORY.createSqlType(typeName);
    }

}
