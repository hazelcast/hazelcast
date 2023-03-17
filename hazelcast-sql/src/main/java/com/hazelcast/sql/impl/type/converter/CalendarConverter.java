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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.internal.serialization.SerializableByConvention;

import java.time.OffsetDateTime;
import java.util.Calendar;

/**
 * Converter for {@link java.util.Calendar} type.
 */
@SerializableByConvention
public final class CalendarConverter extends AbstractTimestampWithTimezoneConverter {

    public static final CalendarConverter INSTANCE = new CalendarConverter();

    private CalendarConverter() {
        super(ID_CALENDAR);
    }

    @Override
    public Class<?> getValueClass() {
        return Calendar.class;
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        Calendar c = (Calendar) val;
        return OffsetDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId());
    }
}
