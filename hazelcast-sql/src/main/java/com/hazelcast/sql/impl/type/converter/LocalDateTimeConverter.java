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
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Converter for {@link LocalDateTime} type.
 */
@SerializableByConvention
public final class LocalDateTimeConverter extends AbstractTemporalConverter {

    public static final LocalDateTimeConverter INSTANCE = new LocalDateTimeConverter();

    private LocalDateTimeConverter() {
        super(ID_LOCAL_DATE_TIME, QueryDataTypeFamily.TIMESTAMP);
    }

    @Override
    public Class<?> getValueClass() {
        return LocalDateTime.class;
    }

    @Override
    public String asVarchar(Object val) {
        return cast(val).toString();
    }

    @Override
    public LocalDate asDate(Object val) {
        return cast(val).toLocalDate();
    }

    @Override
    public LocalTime asTime(Object val) {
        return cast(val).toLocalTime();
    }

    @Override
    public LocalDateTime asTimestamp(Object val) {
        return cast(val);
    }

    @Override
    public OffsetDateTime asTimestampWithTimezone(Object val) {
        return timestampToTimestampWithTimezone(cast(val));
    }

    @Override
    public Object convertToSelf(Converter valConverter, Object val) {
        return valConverter.asTimestamp(val);
    }

    private LocalDateTime cast(Object val) {
        return ((LocalDateTime) val);
    }
}
