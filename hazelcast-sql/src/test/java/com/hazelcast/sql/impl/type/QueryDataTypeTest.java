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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.impl.SqlCustomClass;
import com.hazelcast.sql.impl.expression.RowValue;
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
import com.hazelcast.sql.impl.type.converter.IntervalConverter;
import com.hazelcast.sql.impl.type.converter.JsonConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.LocalTimeConverter;
import com.hazelcast.sql.impl.type.converter.LongConverter;
import com.hazelcast.sql.impl.type.converter.MapConverter;
import com.hazelcast.sql.impl.type.converter.NullConverter;
import com.hazelcast.sql.impl.type.converter.ObjectConverter;
import com.hazelcast.sql.impl.type.converter.OffsetDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.RowConverter;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
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
        checkConverter(new QueryDataType("CustomType"), ObjectConverter.INSTANCE);

        checkConverter(QueryDataType.NULL, NullConverter.INSTANCE);

        checkConverter(QueryDataType.INTERVAL_YEAR_MONTH, IntervalConverter.YEAR_MONTH);
        checkConverter(QueryDataType.INTERVAL_DAY_SECOND, IntervalConverter.DAY_SECOND);

        checkConverter(QueryDataType.MAP, MapConverter.INSTANCE);
        checkConverter(QueryDataType.JSON, JsonConverter.INSTANCE);
        checkConverter(QueryDataType.ROW, RowConverter.INSTANCE);
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

        checkClasses(QueryDataType.INTERVAL_YEAR_MONTH, SqlYearMonthInterval.class);
        checkClasses(QueryDataType.INTERVAL_DAY_SECOND, SqlDaySecondInterval.class);

        checkClasses(QueryDataType.MAP, Map.class, HashMap.class);
        checkClasses(QueryDataType.JSON, HazelcastJsonValue.class);
        checkClasses(QueryDataType.ROW, RowValue.class);
    }

    @Test
    public void testResolutionByName() {
        checkName(QueryDataType.VARCHAR, "VARCHAR");
        checkName(QueryDataType.VARCHAR_CHARACTER, "VARCHAR_CHARACTER");

        checkName(QueryDataType.BOOLEAN, "BOOLEAN");

        checkName(QueryDataType.TINYINT, "TINYINT");
        checkName(QueryDataType.SMALLINT, "SMALLINT");
        checkName(QueryDataType.INT, "INT");
        checkName(QueryDataType.BIGINT, "BIGINT");
        checkName(QueryDataType.DECIMAL, "DECIMAL");
        checkName(QueryDataType.DECIMAL_BIG_INTEGER, "DECIMAL_BIG_INTEGER");
        checkName(QueryDataType.REAL, "REAL");
        checkName(QueryDataType.DOUBLE, "DOUBLE");

        checkName(QueryDataType.TIME, "TIME");
        checkName(QueryDataType.DATE, "DATE");
        checkName(QueryDataType.TIMESTAMP, "TIMESTAMP");
        checkName(QueryDataType.TIMESTAMP_WITH_TZ_DATE, "TIMESTAMP_WITH_TZ_DATE");
        checkName(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, "TIMESTAMP_WITH_TZ_CALENDAR");
        checkName(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, "TIMESTAMP_WITH_TZ_INSTANT");
        checkName(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME, "TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME");
        checkName(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, "TIMESTAMP_WITH_TZ_ZONED_DATE_TIME");

        checkName(QueryDataType.OBJECT, "OBJECT");
        checkName(new QueryDataType("CustomType"), "CustomType");

        checkName(QueryDataType.NULL, "NULL");

        checkName(QueryDataType.INTERVAL_YEAR_MONTH, "INTERVAL_YEAR_MONTH");
        checkName(QueryDataType.INTERVAL_DAY_SECOND, "INTERVAL_DAY_SECOND");

        checkName(QueryDataType.MAP, "MAP");
        checkName(QueryDataType.JSON, "JSON");
        checkName(QueryDataType.ROW, "ROW");
    }

    @Test
    public void testSerialization() {
        for (QueryDataType type : QueryDataType.values()) {
            checkSerialization(type);
        }
        checkSerialization(new QueryDataType("CustomType"));
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

    private void checkConverter(QueryDataType type, Converter converter) {
        assertSame(converter, type.getConverter());
        if (!type.isCustomType()) {
            assertSame(type, QueryDataType.resolveForConverter(converter));
        }
    }

    private void checkClasses(QueryDataType type, Class<?>... classes) {
        assertContains(List.of(classes), type.getConverter().getValueClass());
        for (Class<?> clazz : classes) {
            assertSame(type, QueryDataTypeUtils.resolveTypeForClass(clazz));
        }
    }

    private void checkName(QueryDataType type, String name) {
        assertEquals(name, type.toString());
        if (type.isCustomType()) {
            assertThatThrownBy(() -> QueryDataType.valueOf(name))
                    .hasMessage("No predefined QueryDataType with name " + name);
        } else {
            assertSame(type, QueryDataType.valueOf(name));
        }
    }

    private void checkSerialization(QueryDataType type) {
        Object identifiedDataSerialized = serde(type);
        Object javaSerialized = serde((SupplierEx<Object>) () -> type).get();

        for (Object serialized : List.of(identifiedDataSerialized, javaSerialized)) {
            if (type.isCustomType()) {
                assertEquals(type, serialized);
            } else {
                assertSame(type, serialized);
            }
        }
    }

    private static <T> T serde(T object) {
        return serializationService.toObject(serializationService.toData(object));
    }

    private void checkFamily(QueryDataType expectedType, QueryDataTypeFamily typeFamily) {
        assertSame(expectedType, QueryDataTypeUtils.resolveTypeForTypeFamily(typeFamily));
    }
}
