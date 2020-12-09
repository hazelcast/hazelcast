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
import org.apache.calcite.sql.SqlIdentifier;
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
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isObject;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.isTimestampWithTimeZone;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.narrowestTypeFor;
import static com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeSystem.withHigherPrecedence;
import static com.hazelcast.sql.impl.expression.ExpressionTestBase.TYPE_FACTORY;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.apache.calcite.sql.type.SqlTypeName.ANY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
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
    public void isObjectTest() {
        assertTrue(isObject(new SqlIdentifier("object", ZERO)));
        assertTrue(isObject(new SqlIdentifier("OBJECT", ZERO)));
        assertFalse(isObject(new SqlIdentifier("foo", ZERO)));
    }

    @Test
    public void isTimestampWithTimeZoneTest() {
        assertTrue(isTimestampWithTimeZone(new SqlIdentifier("timestamp_with_time_zone", ZERO)));
        assertTrue(isTimestampWithTimeZone(new SqlIdentifier("TIMESTAMP_WITH_TIME_ZONE", ZERO)));
        assertFalse(isTimestampWithTimeZone(new SqlIdentifier("foo", ZERO)));
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
        assertPrecedence(type(TIME), type(DOUBLE));
        assertPrecedence(type(DATE), type(TIME));
        assertPrecedence(type(TIMESTAMP), type(DATE));
        assertPrecedence(type(TIMESTAMP_WITH_LOCAL_TIME_ZONE), type(TIMESTAMP));
        assertPrecedence(type(ANY), type(TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }

    @Test
    public void narrowestTypeForTest() {
        assertEquals(HazelcastIntegerType.of(0, false), narrowestTypeFor(BigDecimal.valueOf(0), VARCHAR));
        assertEquals(HazelcastIntegerType.of(1, false), narrowestTypeFor(BigDecimal.valueOf(1), DOUBLE));

        assertEquals(type(BIGINT), narrowestTypeFor(new BigDecimal(Long.MAX_VALUE + "0"), BOOLEAN));
        assertEquals(type(DOUBLE), narrowestTypeFor(new BigDecimal(Long.MAX_VALUE + "0"), DOUBLE));

        assertEquals(type(DECIMAL), narrowestTypeFor(BigDecimal.valueOf(0.1), TIME));
        assertEquals(type(DECIMAL), narrowestTypeFor(BigDecimal.valueOf(0.1), DECIMAL));

        assertEquals(type(DOUBLE), narrowestTypeFor(0.1, TIME));
        assertEquals(type(DOUBLE), narrowestTypeFor(0.1, DECIMAL));
        assertEquals(type(REAL), narrowestTypeFor(0.1, REAL));
    }

    @Test
    public void deriveSumTypeTest() {
        assertEquals(type(VARCHAR), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(VARCHAR)));
        assertEquals(type(BOOLEAN), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(BOOLEAN)));
        assertEquals(type(BIGINT), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(TINYINT)));
        assertEquals(type(BIGINT), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(SMALLINT)));
        assertEquals(type(BIGINT), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(INTEGER)));
        assertEquals(type(BIGINT), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(BIGINT)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(DECIMAL)));
        assertEquals(type(DOUBLE), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(REAL)));
        assertEquals(type(DOUBLE), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(DOUBLE)));
        assertEquals(type(TIME), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(TIME)));
        assertEquals(type(DATE), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(DATE)));
        assertEquals(type(TIMESTAMP), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(TIMESTAMP)));
        assertEquals(
                type(TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(TIMESTAMP_WITH_LOCAL_TIME_ZONE))
        );
        assertEquals(type(ANY), HazelcastTypeSystem.INSTANCE.deriveSumType(TYPE_FACTORY, type(ANY)));
    }

    @Test
    public void deriveAvgAggTypeTest() {
        assertEquals(type(VARCHAR), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(VARCHAR)));
        assertEquals(type(BOOLEAN), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(BOOLEAN)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(TINYINT)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(SMALLINT)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(INTEGER)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(BIGINT)));
        assertEquals(type(DECIMAL), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(DECIMAL)));
        assertEquals(type(DOUBLE), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(REAL)));
        assertEquals(type(DOUBLE), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(DOUBLE)));
        assertEquals(type(TIME), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(TIME)));
        assertEquals(type(DATE), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(DATE)));
        assertEquals(type(TIMESTAMP), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(TIMESTAMP)));
        assertEquals(
                type(TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(TIMESTAMP_WITH_LOCAL_TIME_ZONE))
        );
        assertEquals(type(ANY), HazelcastTypeSystem.INSTANCE.deriveAvgAggType(TYPE_FACTORY, type(ANY)));
    }

    private static void assertPrecedence(RelDataType expected, RelDataType other) {
        RelDataType actual = withHigherPrecedence(expected, other);
        assertSame(expected, actual);
        actual = withHigherPrecedence(other, expected);
        assertSame(expected, actual);
    }

    private static RelDataType type(SqlTypeName typeName) {
        return TYPE_FACTORY.createSqlType(typeName);
    }
}
