/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Common converter class for TIMESTAMP WITH TIMEZONE type.
 */
public abstract class AbstractTimestampWithTimezoneConverter extends AbstractTemporalConverter {
    protected AbstractTimestampWithTimezoneConverter(int id) {
        super(id, QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Override
    public Class<?> getNormalizedValueClass() {
        return OffsetDateTime.class;
    }

    @Override
    public final String asVarchar(Object val) {
        return asTimestampWithTimezone(val).toString();
    }

    @Override
    public final LocalDate asDate(Object val) {
        return asTimestamp(val).toLocalDate();
    }

    @Override
    public final LocalTime asTime(Object val) {
        return asTimestamp(val).toLocalTime();
    }

    @Override
    public final LocalDateTime asTimestamp(Object val) {
        return timestampWithTimezoneToTimestamp(asTimestampWithTimezone(val));
    }

    @Override
    public final Object asObject(Object val) {
        return asTimestampWithTimezone(val);
    }

    @Override
    public final Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asTimestampWithTimezone(val);
    }
}
