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

package com.hazelcast.sql.impl.type;

import com.hazelcast.sql.impl.SqlCustomClass;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import com.hazelcast.sql.impl.type.converter.BigIntegerConverter;
import com.hazelcast.sql.impl.type.converter.BooleanConverter;
import com.hazelcast.sql.impl.type.converter.ByteConverter;
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import com.hazelcast.sql.impl.type.converter.CharacterConverter;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import com.hazelcast.sql.impl.type.converter.DateConverter;
import com.hazelcast.sql.impl.type.converter.DoubleConverter;
import com.hazelcast.sql.impl.type.converter.FloatConverter;
import com.hazelcast.sql.impl.type.converter.InstantConverter;
import com.hazelcast.sql.impl.type.converter.IntegerConverter;
import com.hazelcast.sql.impl.type.converter.LateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.LocalTimeConverter;
import com.hazelcast.sql.impl.type.converter.LongConverter;
import com.hazelcast.sql.impl.type.converter.ObjectConverter;
import com.hazelcast.sql.impl.type.converter.OffsetDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.ShortConverter;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import com.hazelcast.sql.impl.type.converter.ZonedDateTimeConverter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryDataTypeTest extends SqlTestSupport {
    @Test
    public void testDefaultTypes() {
        checkType(QueryDataType.LATE, LateConverter.INSTANCE);

        checkType(QueryDataType.VARCHAR, StringConverter.INSTANCE);
        checkType(QueryDataType.VARCHAR_CHARACTER, CharacterConverter.INSTANCE);

        checkType(QueryDataType.BOOLEAN, BooleanConverter.INSTANCE, QueryDataType.PRECISION_BOOLEAN);
        checkType(QueryDataType.TINYINT, ByteConverter.INSTANCE, QueryDataType.PRECISION_TINYINT);
        checkType(QueryDataType.SMALLINT, ShortConverter.INSTANCE, QueryDataType.PRECISION_SMALLINT);
        checkType(QueryDataType.INT, IntegerConverter.INSTANCE, QueryDataType.PRECISION_INT);
        checkType(QueryDataType.BIGINT, LongConverter.INSTANCE, QueryDataType.PRECISION_BIGINT);
        checkType(QueryDataType.DECIMAL, BigDecimalConverter.INSTANCE);
        checkType(QueryDataType.DECIMAL_BIG_INTEGER, BigIntegerConverter.INSTANCE);
        checkType(QueryDataType.REAL, FloatConverter.INSTANCE);
        checkType(QueryDataType.DOUBLE, DoubleConverter.INSTANCE);

        checkType(QueryDataType.TIME, LocalTimeConverter.INSTANCE);
        checkType(QueryDataType.DATE, LocalDateConverter.INSTANCE);
        checkType(QueryDataType.TIMESTAMP, LocalDateTimeConverter.INSTANCE);
        checkType(QueryDataType.TIMESTAMP_WITH_TZ_DATE, DateConverter.INSTANCE);
        checkType(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, CalendarConverter.INSTANCE);
        checkType(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, InstantConverter.INSTANCE);
        checkType(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, OffsetDateTimeConverter.INSTANCE);
        checkType(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, ZonedDateTimeConverter.INSTANCE);

        checkType(QueryDataType.OBJECT, ObjectConverter.INSTANCE);
    }

    @Test
    public void testIntegerTypeFactory() {
        for (int i = 1; i <= QueryDataType.PRECISION_BIGINT; i++) {
            QueryDataType type = QueryDataTypeUtils.integerType(i);

            int precision = type.getPrecision();

            assertEquals(i, precision);

            if (precision <= QueryDataType.PRECISION_BOOLEAN) {
                assertEquals(QueryDataTypeFamily.BOOLEAN, type.getTypeFamily());
            } else if (precision <= QueryDataType.PRECISION_TINYINT) {
                assertEquals(QueryDataTypeFamily.TINYINT, type.getTypeFamily());
            } else if (precision <= QueryDataType.PRECISION_SMALLINT) {
                assertEquals(QueryDataTypeFamily.SMALLINT, type.getTypeFamily());
            } else if (precision <= QueryDataType.PRECISION_INT) {
                assertEquals(QueryDataTypeFamily.INT, type.getTypeFamily());
            } else if (precision <= QueryDataType.PRECISION_BIGINT) {
                assertEquals(QueryDataTypeFamily.BIGINT, type.getTypeFamily());
            }
        }

        QueryDataType decimalType = QueryDataTypeUtils.integerType(QueryDataType.PRECISION_BIGINT + 1);
        checkType(decimalType, decimalType.getConverter(), QueryDataType.PRECISION_UNLIMITED);

        decimalType = QueryDataTypeUtils.integerType(QueryDataType.PRECISION_UNLIMITED);
        checkType(decimalType, decimalType.getConverter(), QueryDataType.PRECISION_UNLIMITED);
    }

    @Test
    public void testTypeResolutionByValue() {
        assertEquals(QueryDataType.LATE, QueryDataTypeUtils.resolveType(null));
        assertEquals(QueryDataType.INT, QueryDataTypeUtils.resolveType(1));
    }

    @Test
    public void testTypeResolutionByClass() {
        checkResolvedTypeForClass(QueryDataType.VARCHAR, String.class);
        checkResolvedTypeForClass(QueryDataType.VARCHAR_CHARACTER, char.class, Character.class);

        checkResolvedTypeForClass(QueryDataType.BOOLEAN, boolean.class, Boolean.class);
        checkResolvedTypeForClass(QueryDataType.TINYINT, byte.class, Byte.class);
        checkResolvedTypeForClass(QueryDataType.SMALLINT, short.class, Short.class);
        checkResolvedTypeForClass(QueryDataType.INT, int.class, Integer.class);
        checkResolvedTypeForClass(QueryDataType.BIGINT, long.class, Long.class);
        checkResolvedTypeForClass(QueryDataType.DECIMAL, BigDecimal.class);
        checkResolvedTypeForClass(QueryDataType.DECIMAL_BIG_INTEGER, BigInteger.class);
        checkResolvedTypeForClass(QueryDataType.REAL, float.class, Float.class);
        checkResolvedTypeForClass(QueryDataType.DOUBLE, double.class, Double.class);

        checkResolvedTypeForClass(QueryDataType.TIME, LocalTime.class);
        checkResolvedTypeForClass(QueryDataType.DATE, LocalDate.class);
        checkResolvedTypeForClass(QueryDataType.TIMESTAMP, LocalDateTime.class);
        checkResolvedTypeForClass(QueryDataType.TIMESTAMP_WITH_TZ_DATE, Date.class);
        checkResolvedTypeForClass(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, Calendar.class, GregorianCalendar.class);
        checkResolvedTypeForClass(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, Instant.class);
        checkResolvedTypeForClass(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, OffsetDateTime.class);
        checkResolvedTypeForClass(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, ZonedDateTime.class);

        checkResolvedTypeForClass(QueryDataType.OBJECT, Object.class, SqlCustomClass.class);
    }

    @Test
    public void testTypeResolutionByFamily() {
        checkResolvedTypeForTypeFamily(QueryDataType.VARCHAR, QueryDataTypeFamily.VARCHAR);

        checkResolvedTypeForTypeFamily(QueryDataType.BOOLEAN, QueryDataTypeFamily.BOOLEAN);
        checkResolvedTypeForTypeFamily(QueryDataType.TINYINT, QueryDataTypeFamily.TINYINT);
        checkResolvedTypeForTypeFamily(QueryDataType.SMALLINT, QueryDataTypeFamily.SMALLINT);
        checkResolvedTypeForTypeFamily(QueryDataType.INT, QueryDataTypeFamily.INT);
        checkResolvedTypeForTypeFamily(QueryDataType.BIGINT, QueryDataTypeFamily.BIGINT);
        checkResolvedTypeForTypeFamily(QueryDataType.DECIMAL, QueryDataTypeFamily.DECIMAL);
        checkResolvedTypeForTypeFamily(QueryDataType.REAL, QueryDataTypeFamily.REAL);
        checkResolvedTypeForTypeFamily(QueryDataType.DOUBLE, QueryDataTypeFamily.DOUBLE);

        checkResolvedTypeForTypeFamily(QueryDataType.TIME, QueryDataTypeFamily.TIME);
        checkResolvedTypeForTypeFamily(QueryDataType.DATE, QueryDataTypeFamily.DATE);
        checkResolvedTypeForTypeFamily(QueryDataType.TIMESTAMP, QueryDataTypeFamily.TIMESTAMP);
        checkResolvedTypeForTypeFamily(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME,
            QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE);

        checkResolvedTypeForTypeFamily(QueryDataType.OBJECT, QueryDataTypeFamily.OBJECT);
    }

    @Test
    public void testBigger() {
        checkPrecedence(QueryDataType.VARCHAR, QueryDataType.LATE);

        checkPrecedence(QueryDataType.BOOLEAN, QueryDataType.VARCHAR);
        checkPrecedence(QueryDataType.TINYINT, QueryDataType.BOOLEAN);
        checkPrecedence(QueryDataType.SMALLINT, QueryDataType.TINYINT);
        checkPrecedence(QueryDataType.INT, QueryDataType.SMALLINT);
        checkPrecedence(QueryDataType.BIGINT, QueryDataType.INT);
        checkPrecedence(QueryDataType.DECIMAL, QueryDataType.BIGINT);
        checkPrecedence(QueryDataType.REAL, QueryDataType.DECIMAL);
        checkPrecedence(QueryDataType.DOUBLE, QueryDataType.REAL);

        checkPrecedence(QueryDataType.TIME, QueryDataType.DOUBLE);
        checkPrecedence(QueryDataType.DATE, QueryDataType.TIME);
        checkPrecedence(QueryDataType.TIMESTAMP, QueryDataType.DATE);
        checkPrecedence(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, QueryDataType.TIMESTAMP);

        for (int i = 1; i < QueryDataType.PRECISION_BIGINT; i++) {
            checkPrecedence(QueryDataTypeUtils.integerType(i + 1), QueryDataTypeUtils.integerType(i));
        }
    }

    @Test
    public void testEquals() {
        checkEquals(new QueryDataType(IntegerConverter.INSTANCE, 11), new QueryDataType(IntegerConverter.INSTANCE, 11), true);
        checkEquals(new QueryDataType(IntegerConverter.INSTANCE, 11), new QueryDataType(IntegerConverter.INSTANCE, 12), false);
        checkEquals(new QueryDataType(IntegerConverter.INSTANCE, 11), new QueryDataType(LongConverter.INSTANCE, 11), false);
    }

    @Test
    public void testSerialization() {
        for (Converter converter : Converters.getConverters()) {
            QueryDataType original = new QueryDataType(converter, QueryDataType.PRECISION_BIGINT);
            QueryDataType restored = serializeAndCheck(original, SqlDataSerializerHook.QUERY_DATA_TYPE);

            checkEquals(original, restored, true);
        }
    }

    private void checkType(QueryDataType type, Converter expectedConverter) {
        checkType(type, expectedConverter, QueryDataType.PRECISION_UNLIMITED);
    }

    private void checkType(QueryDataType type, Converter expectedConverter, int expectedPrecision) {
        assertEquals(expectedConverter, type.getConverter());
        assertEquals(expectedConverter.getTypeFamily(), type.getConverter().getTypeFamily());
        assertEquals(expectedPrecision, type.getPrecision());
    }

    private void checkResolvedTypeForClass(QueryDataType expectedType, Class<?>... classes) {
        for (Class<?> clazz : classes) {
            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(clazz);

            assertSame(expectedType, type);
        }
    }

    private void checkResolvedTypeForTypeFamily(QueryDataType expectedType, QueryDataTypeFamily typeFamily) {
        assertSame(expectedType, QueryDataTypeUtils.resolveTypeForTypeFamily(typeFamily));
    }

    private void checkPrecedence(QueryDataType bigger, QueryDataType smaller) {
        assertSame(bigger, QueryDataTypeUtils.withHigherPrecedence(bigger, smaller));

        assertSame(bigger, QueryDataTypeUtils.withHigherPrecedence(bigger, bigger));
        assertSame(smaller, QueryDataTypeUtils.withHigherPrecedence(smaller, smaller));
    }
}
