/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.type;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.impl.SqlCustomClass;
import com.hazelcast.sql.impl.type.converter.BigDecimalConverter;
import com.hazelcast.sql.impl.type.converter.BigIntegerConverter;
import com.hazelcast.sql.impl.type.converter.BooleanConverter;
import com.hazelcast.sql.impl.type.converter.ByteConverter;
import com.hazelcast.sql.impl.type.converter.CalendarConverter;
import com.hazelcast.sql.impl.type.converter.CharacterConverter;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.DateConverter;
import com.hazelcast.sql.impl.type.converter.DoubleConverter;
import com.hazelcast.sql.impl.type.converter.FloatConverter;
import com.hazelcast.sql.impl.type.converter.InstantConverter;
import com.hazelcast.sql.impl.type.converter.IntegerConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.LocalTimeConverter;
import com.hazelcast.sql.impl.type.converter.LongConverter;
import com.hazelcast.sql.impl.type.converter.NullConverter;
import com.hazelcast.sql.impl.type.converter.ObjectConverter;
import com.hazelcast.sql.impl.type.converter.OffsetDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.ShortConverter;
import com.hazelcast.sql.impl.type.converter.StringConverter;
import com.hazelcast.sql.impl.type.converter.ZonedDateTimeConverter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryDataTypeTest extends HazelcastTestSupport {
    private static InternalSerializationService serializationService;

    @BeforeClass
    public static void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @AfterClass
    public static void tearDown() {
        serializationService.dispose();
    }

    @Test
    public void testResolutionByConverter() {
        checkConverter(QueryDataType.VARCHAR, StringConverter.INSTANCE);
        checkConverter(QueryDataType.VARCHAR_CHARACTER, CharacterConverter.INSTANCE);

        checkConverter(QueryDataType.BOOLEAN, BooleanConverter.INSTANCE);

        checkConverter(QueryDataType.TINYINT, ByteConverter.INSTANCE);
        checkConverter(QueryDataType.SMALLINT, ShortConverter.INSTANCE);
        checkConverter(QueryDataType.INT, IntegerConverter.INSTANCE);
        checkConverter(QueryDataType.BIGINT, LongConverter.INSTANCE);
        checkConverter(QueryDataType.DECIMAL, BigDecimalConverter.INSTANCE);
        checkConverter(QueryDataType.DECIMAL_BIG_INTEGER, BigIntegerConverter.INSTANCE);
        checkConverter(QueryDataType.REAL, FloatConverter.INSTANCE);
        checkConverter(QueryDataType.DOUBLE, DoubleConverter.INSTANCE);

        checkConverter(QueryDataType.TIME, LocalTimeConverter.INSTANCE);
        checkConverter(QueryDataType.DATE, LocalDateConverter.INSTANCE);
        checkConverter(QueryDataType.TIMESTAMP, LocalDateTimeConverter.INSTANCE);
        checkConverter(QueryDataType.TIMESTAMP_WITH_TZ_DATE, DateConverter.INSTANCE);
        checkConverter(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, CalendarConverter.INSTANCE);
        checkConverter(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, InstantConverter.INSTANCE);
        checkConverter(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, OffsetDateTimeConverter.INSTANCE);
        checkConverter(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, ZonedDateTimeConverter.INSTANCE);

        checkConverter(QueryDataType.OBJECT, ObjectConverter.INSTANCE);

        checkConverter(QueryDataType.NULL, NullConverter.INSTANCE);
    }

    @Test
    public void testResolutionByClass() {
        checkClasses(QueryDataType.VARCHAR, String.class);
        checkClasses(QueryDataType.VARCHAR_CHARACTER, char.class, Character.class);

        checkClasses(QueryDataType.BOOLEAN, boolean.class, Boolean.class);

        checkClasses(QueryDataType.TINYINT, byte.class, Byte.class);
        checkClasses(QueryDataType.SMALLINT, short.class, Short.class);
        checkClasses(QueryDataType.INT, int.class, Integer.class);
        checkClasses(QueryDataType.BIGINT, long.class, Long.class);
        checkClasses(QueryDataType.DECIMAL, BigDecimal.class);
        checkClasses(QueryDataType.DECIMAL_BIG_INTEGER, BigInteger.class);
        checkClasses(QueryDataType.REAL, float.class, Float.class);
        checkClasses(QueryDataType.DOUBLE, double.class, Double.class);

        checkClasses(QueryDataType.TIME, LocalTime.class);
        checkClasses(QueryDataType.DATE, LocalDate.class);
        checkClasses(QueryDataType.TIMESTAMP, LocalDateTime.class);
        checkClasses(QueryDataType.TIMESTAMP_WITH_TZ_DATE, Date.class);
        checkClasses(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, Calendar.class, GregorianCalendar.class);
        checkClasses(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, Instant.class);
        checkClasses(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, OffsetDateTime.class);
        checkClasses(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, ZonedDateTime.class);

        checkClasses(QueryDataType.OBJECT, Object.class, SqlCustomClass.class);

        checkClasses(QueryDataType.NULL, void.class, Void.class);
    }

    @Test
    public void testSerialization() {
        checkSerialization(QueryDataType.VARCHAR);
        checkSerialization(QueryDataType.VARCHAR_CHARACTER);

        checkSerialization(QueryDataType.BOOLEAN);

        checkSerialization(QueryDataType.TINYINT);
        checkSerialization(QueryDataType.SMALLINT);
        checkSerialization(QueryDataType.INT);
        checkSerialization(QueryDataType.BIGINT);
        checkSerialization(QueryDataType.DECIMAL);
        checkSerialization(QueryDataType.DECIMAL_BIG_INTEGER);
        checkSerialization(QueryDataType.REAL);
        checkSerialization(QueryDataType.DOUBLE);

        checkSerialization(QueryDataType.TIME);
        checkSerialization(QueryDataType.DATE);
        checkSerialization(QueryDataType.TIMESTAMP);
        checkSerialization(QueryDataType.TIMESTAMP_WITH_TZ_DATE);
        checkSerialization(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR);
        checkSerialization(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT);
        checkSerialization(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);
        checkSerialization(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME);

        checkSerialization(QueryDataType.OBJECT);

        checkSerialization(QueryDataType.NULL);

        checkSerialization(QueryDataType.INTERVAL_YEAR_MONTH);
        checkSerialization(QueryDataType.INTERVAL_DAY_SECOND);

        checkSerialization(QueryDataType.MAP);
        checkSerialization(QueryDataType.JSON);
        checkSerialization(QueryDataType.ROW);
    }

    @Test
    public void testEquals() {
        assertEquals(new QueryDataType(IntegerConverter.INSTANCE), new QueryDataType(IntegerConverter.INSTANCE));
        assertNotEquals(new QueryDataType(IntegerConverter.INSTANCE), new QueryDataType(LongConverter.INSTANCE));
    }

    // TODO This test will be removed by HZ-3691.
    @Test
    public void testResolutionByFamily() {
        checkFamily(QueryDataType.VARCHAR, QueryDataTypeFamily.VARCHAR);

        checkFamily(QueryDataType.BOOLEAN, QueryDataTypeFamily.BOOLEAN);
        checkFamily(QueryDataType.TINYINT, QueryDataTypeFamily.TINYINT);
        checkFamily(QueryDataType.SMALLINT, QueryDataTypeFamily.SMALLINT);
        checkFamily(QueryDataType.INT, QueryDataTypeFamily.INTEGER);
        checkFamily(QueryDataType.BIGINT, QueryDataTypeFamily.BIGINT);
        checkFamily(QueryDataType.DECIMAL, QueryDataTypeFamily.DECIMAL);
        checkFamily(QueryDataType.REAL, QueryDataTypeFamily.REAL);
        checkFamily(QueryDataType.DOUBLE, QueryDataTypeFamily.DOUBLE);

        checkFamily(QueryDataType.TIME, QueryDataTypeFamily.TIME);
        checkFamily(QueryDataType.DATE, QueryDataTypeFamily.DATE);
        checkFamily(QueryDataType.TIMESTAMP, QueryDataTypeFamily.TIMESTAMP);
        checkFamily(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME,
                QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE);

        checkFamily(QueryDataType.OBJECT, QueryDataTypeFamily.OBJECT);

        checkFamily(QueryDataType.NULL, QueryDataTypeFamily.NULL);
    }

    private void checkConverter(QueryDataType type, Converter expectedConverter) {
        assertSame(expectedConverter, type.getConverter());
        assertSame(expectedConverter.getTypeFamily(), type.getConverter().getTypeFamily());
    }

    private void checkClasses(QueryDataType expectedType, Class<?>... classes) {
        for (Class<?> clazz : classes) {
            assertSame(expectedType, QueryDataTypeUtils.resolveTypeForClass(clazz));
        }
    }

    private void checkSerialization(QueryDataType type) {
        assertEquals(type, serializationService.toObject(serializationService.toData(type)));
    }

    private void checkFamily(QueryDataType expectedType, QueryDataTypeFamily typeFamily) {
        assertSame(expectedType, QueryDataTypeUtils.resolveTypeForTypeFamily(typeFamily));
    }
}
