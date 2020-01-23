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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Converter for {@link LocalDate} type.
 */
public final class LocalDateConverter extends Converter {
    /** Singleton instance. */
    public static final LocalDateConverter INSTANCE = new LocalDateConverter();

    private LocalDateConverter() {
        // No-op.
    }

    @Override
    public Class getClazz() {
        return LocalDate.class;
    }

    @Override
    public GenericType getGenericType() {
        return GenericType.DATE;
    }

    @Override
    public String asVarchar(Object val) {
        return cast(val).toString();
    }

    @Override
    public LocalDate asDate(Object val) {
        return cast(val);
    }

    @Override
    public LocalDateTime asTimestamp(Object val) {
        return cast(val).atStartOfDay();
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        return ZonedDateTime.of(asTimestamp(val), ZoneId.systemDefault()).toOffsetDateTime();
    }

    private LocalDate cast(Object val) {
        return ((LocalDate) val);
    }
}
