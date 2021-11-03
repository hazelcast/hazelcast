/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class ToConverters {

    private static final Map<QueryDataType, ToConverter> CONVERTERS = prepareConverters();

    private ToConverters() {
    }

    @Nonnull
    public static ToConverter getToConverter(QueryDataType type) {
        return Objects.requireNonNull(CONVERTERS.get(type), "missing converter for " + type);
    }

    private static Map<QueryDataType, ToConverter> prepareConverters() {
        Map<QueryDataType, ToConverter> converters = new HashMap<>();

        converters.put(QueryDataType.BOOLEAN, new ToCanonicalConverter(QueryDataType.BOOLEAN));
        converters.put(QueryDataType.TINYINT, new ToCanonicalConverter(QueryDataType.TINYINT));
        converters.put(QueryDataType.SMALLINT, new ToCanonicalConverter(QueryDataType.SMALLINT));
        converters.put(QueryDataType.INT, new ToCanonicalConverter(QueryDataType.INT));
        converters.put(QueryDataType.BIGINT, new ToCanonicalConverter(QueryDataType.BIGINT));
        converters.put(QueryDataType.REAL, new ToCanonicalConverter(QueryDataType.REAL));
        converters.put(QueryDataType.DOUBLE, new ToCanonicalConverter(QueryDataType.DOUBLE));
        converters.put(QueryDataType.DECIMAL, new ToCanonicalConverter(QueryDataType.DECIMAL));
        converters.put(QueryDataType.DECIMAL_BIG_INTEGER, ToDecimalBigIntegerConverter.INSTANCE);

        converters.put(QueryDataType.VARCHAR_CHARACTER, ToVarcharCharacterConverter.INSTANCE);
        converters.put(QueryDataType.VARCHAR, new ToCanonicalConverter(QueryDataType.VARCHAR));

        converters.put(QueryDataType.TIME, new ToCanonicalConverter(QueryDataType.TIME));
        converters.put(QueryDataType.DATE, new ToCanonicalConverter(QueryDataType.DATE));
        converters.put(QueryDataType.TIMESTAMP, new ToCanonicalConverter(QueryDataType.TIMESTAMP));
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME, ToZonedDateTimeConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME,
                new ToCanonicalConverter(QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME));
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_DATE, ToDateConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT, ToInstantConverter.INSTANCE);
        converters.put(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR, ToCalendarConverter.INSTANCE);

        converters.put(QueryDataType.OBJECT, new ToCanonicalConverter(QueryDataType.OBJECT));
        converters.put(QueryDataType.JSON, new ToCanonicalConverter(QueryDataType.JSON));

        return converters;
    }

    /**
     * Returns a {@code ToConverter} that converts to the canonical class of
     * the given {@code QueryDataType}.
     */
    private static final class ToCanonicalConverter extends ToConverter {

        private ToCanonicalConverter(QueryDataType type) {
            super(type);
        }

        @Override
        public Object from(Object canonicalValue) {
            return canonicalValue;
        }
    }

    private static final class ToVarcharCharacterConverter extends ToConverter {

        private static final ToVarcharCharacterConverter INSTANCE = new ToVarcharCharacterConverter();

        private ToVarcharCharacterConverter() {
            super(QueryDataType.VARCHAR_CHARACTER);
        }

        @Override
        public Object from(Object canonicalValue) {
            return ((String) canonicalValue).charAt(0);
        }
    }

    private static final class ToDecimalBigIntegerConverter extends ToConverter {

        private static final ToDecimalBigIntegerConverter INSTANCE = new ToDecimalBigIntegerConverter();

        private ToDecimalBigIntegerConverter() {
            super(QueryDataType.DECIMAL_BIG_INTEGER);
        }

        @Override
        public Object from(Object canonicalValue) {
            return ((BigDecimal) canonicalValue).toBigInteger();
        }
    }

    private static final class ToDateConverter extends ToConverter {

        private static final ToDateConverter INSTANCE = new ToDateConverter();

        private ToDateConverter() {
            super(QueryDataType.TIMESTAMP_WITH_TZ_DATE);
        }

        @Override
        public Object from(Object canonicalValue) {
            Instant instant = ((OffsetDateTime) canonicalValue).toInstant();
            return Date.from(instant);
        }
    }

    private static final class ToCalendarConverter extends ToConverter {

        private static final ToCalendarConverter INSTANCE = new ToCalendarConverter();

        private ToCalendarConverter() {
            super(QueryDataType.TIMESTAMP_WITH_TZ_CALENDAR);
        }

        @Override
        public Object from(Object canonicalValue) {
            ZonedDateTime zdt = ((OffsetDateTime) canonicalValue).toZonedDateTime();
            return GregorianCalendar.from(zdt);
        }
    }

    private static final class ToInstantConverter extends ToConverter {

        private static final ToInstantConverter INSTANCE = new ToInstantConverter();

        private ToInstantConverter() {
            super(QueryDataType.TIMESTAMP_WITH_TZ_INSTANT);
        }

        @Override
        public Object from(Object canonicalValue) {
            return ((OffsetDateTime) canonicalValue).toInstant();
        }
    }

    private static final class ToZonedDateTimeConverter extends ToConverter {

        private static final ToZonedDateTimeConverter INSTANCE =
                new ToZonedDateTimeConverter();

        private ToZonedDateTimeConverter() {
            super(QueryDataType.TIMESTAMP_WITH_TZ_ZONED_DATE_TIME);
        }

        @Override
        public Object from(Object canonicalValue) {
            return ((OffsetDateTime) canonicalValue).toZonedDateTime();
        }
    }
}
