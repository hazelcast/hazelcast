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

package com.hazelcast.sql.impl.type.accessor;

import com.hazelcast.sql.impl.type.GenericType;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;

/**
 * Converter for {@link java.util.Calendar} type.
 */
public final class CalendarConverter extends Converter {
    /** Singleton instance. */
    public static final CalendarConverter INSTANCE = new CalendarConverter();

    private CalendarConverter() {
        // No-op.
    }

    @Override
    public Class getClazz() {
        return Calendar.class;
    }

    @Override
    public GenericType getGenericType() {
        return GenericType.TIMESTAMP_WITH_TIMEZONE;
    }

    @Override
    public String asVarchar(Object val) {
        return asTimestampWithTimezone(val).toString();
    }

    @Override
    public LocalDate asDate(Object val) {
        return asTimestamp(val).toLocalDate();
    }

    @Override
    public LocalTime asTime(Object val) {
        return asTimestamp(val).toLocalTime();
    }

    @Override
    public LocalDateTime asTimestamp(Object val) {
        Instant instant = cast(val).toInstant();

        return LocalDateTime.ofInstant(instant, ZoneOffset.systemDefault());
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        Instant instant = cast(val).toInstant();

        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private Calendar cast(Object val) {
        return ((Calendar) val);
    }
}
